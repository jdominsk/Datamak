import AppKit
import SwiftUI

@main
enum DatamakMenuBar {
    @MainActor
    static func main() {
        let app = NSApplication.shared
        let delegate = MenuBarDelegate()
        app.delegate = delegate
        app.setActivationPolicy(delegate.opensWindow ? .regular : .accessory)
        withExtendedLifetime(delegate) { app.run() }
    }
}

private struct SnapshotReader {
    let python: String
    let script: URL?
    let liteRoot: String
    let configDirectory: String?
    let isDemo: Bool

    enum ReadError: LocalizedError {
        case missingScript
        case processFailed(String)

        var errorDescription: String? {
            switch self {
            case .missingScript:
                return "The menu bar app is missing its Datamak snapshot reader."
            case .processFailed(let detail):
                return detail.isEmpty ? "The Datamak snapshot reader stopped." : detail
            }
        }
    }

    init(bundle: Bundle) {
        let environment = ProcessInfo.processInfo.environment
        python = bundle.object(forInfoDictionaryKey: "DatamakPython") as? String ?? "/usr/bin/python3"
        script = bundle.url(forResource: "snapshot", withExtension: "py")
        liteRoot = environment["DATAMAK_LITE_ROOT"]
            ?? bundle.object(forInfoDictionaryKey: "DatamakLiteRoot") as? String
            ?? ""
        configDirectory = environment["DATAMAK_HOME"]
            ?? bundle.object(forInfoDictionaryKey: "DatamakHome") as? String
        isDemo = bundle.object(forInfoDictionaryKey: "DatamakDemo") as? Bool ?? false
    }

    func read() throws -> Snapshot {
        guard let script else { throw ReadError.missingScript }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: python)
        var arguments = ["-B", script.path]
        if isDemo { arguments.append("--demo") }
        if let configDirectory, !configDirectory.isEmpty {
            arguments += ["--config-dir", configDirectory]
        }
        process.arguments = arguments
        var environment = ProcessInfo.processInfo.environment
        environment["PYTHONPATH"] = liteRoot
        process.environment = environment

        let output = Pipe()
        process.standardOutput = output
        process.standardError = output
        try process.run()

        let terminateOnTimeout = DispatchWorkItem {
            if process.isRunning { process.terminate() }
        }
        DispatchQueue.global().asyncAfter(deadline: .now() + 20, execute: terminateOnTimeout)
        defer { terminateOnTimeout.cancel() }

        let data = output.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else {
            let diagnostic = String(decoding: data, as: UTF8.self)
                .split(whereSeparator: \.isNewline).last.map(String.init) ?? ""
            let message = process.terminationReason == .uncaughtSignal
                ? "The local Datamak read timed out or was interrupted."
                : String(diagnostic.prefix(240))
            throw ReadError.processFailed(message)
        }

        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        return try decoder.decode(Snapshot.self, from: data)
    }
}

@MainActor
private final class MenuBarState: ObservableObject {
    @Published var snapshot: Snapshot?
    @Published var error: String?
    @Published var isRefreshing = false
    @Published var lastChecked: Date?
    @Published var hasChanges = false
    @Published var archiveQueueReadError: String?
}

private struct MenuBarContent: View {
    @ObservedObject var state: MenuBarState
    let isDemo: Bool
    let onRefresh: () -> Void
    let onHide: () -> Void
    let archiveQueue: ArchiveQueue
    let remoteQuotas: RemoteQuotaStore
    let resizesVertically: Bool

    var body: some View {
        Dashboard(snapshot: state.snapshot, error: state.error, isRefreshing: state.isRefreshing,
                  isDemo: isDemo, lastChecked: state.lastChecked, hasChanges: state.hasChanges,
                  onRefresh: onRefresh, onHide: onHide, archiveQueue: archiveQueue,
                  archiveQueueReadError: state.archiveQueueReadError, remoteQuotas: remoteQuotas,
                  resizesVertically: resizesVertically)
    }
}

@MainActor
private final class MenuBarDelegate: NSObject, NSApplicationDelegate, NSPopoverDelegate {
    private let state = MenuBarState()
    private let reader = SnapshotReader(bundle: .main)
    private lazy var archiveQueue = ArchiveQueue(isDemo: reader.isDemo)
    private lazy var remoteQuotas = RemoteQuotaStore(bundle: .main, python: reader.python, enabled: !reader.isDemo)
    private lazy var appUpdate = AppUpdate(
        bundle: .main, python: reader.python, liteRoot: reader.liteRoot,
        configDirectory: reader.configDirectory, opensWindow: opensWindow
    )
    private var updateMenuItem: NSMenuItem?
    private var updateInProgress = false
    private var installer: Process?
    private let popover = NSPopover()
    private var statusItem: NSStatusItem?
    private var dashboardWindow: NSWindow?
    private var pollTimer: Timer?
    private var seenSnapshot: Snapshot?
    private var changedCampaignNames: [String] = []

    var opensWindow: Bool {
        reader.isDemo || Bundle.main.object(forInfoDictionaryKey: "DatamakDashboardWindow") as? Bool == true
    }

    func applicationDidFinishLaunching(_ notification: Notification) {
        let content = MenuBarContent(state: state, isDemo: reader.isDemo, onRefresh: { [weak self] in
            self?.refresh()
        }, onHide: { [weak self] in
            self?.hideDashboard()
        }, archiveQueue: archiveQueue, remoteQuotas: remoteQuotas, resizesVertically: opensWindow)
        let controller = NSHostingController(rootView: content)
        if opensWindow {
            controller.sizingOptions = [.minSize, .maxSize]
            let window = NSWindow(contentViewController: controller)
            let title = Bundle.main.object(forInfoDictionaryKey: "CFBundleDisplayName") as? String ?? "Datamak Lite"
            window.title = title
            window.styleMask = [.titled, .closable, .miniaturizable, .resizable]
            window.setContentSize(Dashboard.defaultSize)
            window.isReleasedWhenClosed = false
            window.center()
            dashboardWindow = window
            installApplicationMenu(title: title)
            showDashboardWindow()
        } else {
            let item = NSStatusBar.system.statusItem(withLength: 31)
            if let button = item.button {
                button.target = self
                button.action = #selector(statusItemPressed(_:))
                button.sendAction(on: [.leftMouseUp, .rightMouseUp])
                button.imagePosition = .imageOnly
            }
            statusItem = item
            popover.contentViewController = controller
            popover.contentSize = Dashboard.defaultSize
            popover.behavior = .transient
            popover.delegate = self
        }

        refresh()
        pollTimer = Timer.scheduledTimer(timeInterval: 60, target: self, selector: #selector(refresh),
                                         userInfo: nil, repeats: true)
    }

    func applicationWillTerminate(_ notification: Notification) {
        pollTimer?.invalidate()
    }

    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows: Bool) -> Bool {
        if opensWindow { showDashboardWindow() }
        return false
    }

    private func showDashboardWindow() {
        dashboardWindow?.deminiaturize(nil)
        dashboardWindow?.makeKeyAndOrderFront(nil)
        NSApp.activate()
    }

    private func hideDashboard() {
        if opensWindow {
            dashboardWindow?.orderOut(nil)
        } else {
            popover.performClose(nil)
        }
    }

    private func installApplicationMenu(title: String) {
        let menuBar = NSMenu()
        let application = NSMenuItem()
        let submenu = NSMenu()
        submenu.autoenablesItems = false
        if appUpdate != nil {
            updateMenuItem = addUpdateItem(to: submenu)
            submenu.addItem(.separator())
        }
        let quit = submenu.addItem(withTitle: "Quit \(title)", action: #selector(NSApplication.terminate(_:)),
                                   keyEquivalent: "q")
        quit.target = NSApp
        application.submenu = submenu
        menuBar.addItem(application)
        NSApp.mainMenu = menuBar
    }

    @objc private func statusItemPressed(_ button: NSStatusBarButton) {
        let below: NSRectEdge = button.isFlipped ? .maxY : .minY
        let event = NSApp.currentEvent
        if event?.type == .rightMouseUp || event?.modifierFlags.contains(.control) == true {
            if popover.isShown { hideDashboard() }
            let menu = NSMenu()
            menu.autoenablesItems = false
            if appUpdate != nil {
                addUpdateItem(to: menu)
                menu.addItem(.separator())
            }
            let quit = menu.addItem(withTitle: "Quit Datamak Lite", action: #selector(NSApplication.terminate(_:)),
                                    keyEquivalent: "")
            quit.target = NSApp
            menu.popUp(positioning: nil, at: .init(x: button.bounds.minX,
                                                  y: button.isFlipped ? button.bounds.maxY : button.bounds.minY),
                       in: button)
            return
        }
        if popover.isShown {
            hideDashboard()
        } else {
            popover.show(relativeTo: button.bounds, of: button, preferredEdge: below)
            refresh()
        }
    }

    func popoverDidClose(_ notification: Notification) {
        seenSnapshot = state.snapshot
        state.hasChanges = false
        changedCampaignNames = []
        updateStatusItem()
    }

    @objc private func refresh() {
        checkForUpdates(manual: false)
        guard !state.isRefreshing else { return }
        do {
            try archiveQueue.reload()
            state.archiveQueueReadError = nil
        } catch {
            state.archiveQueueReadError = error.localizedDescription
        }
        state.isRefreshing = true
        updateStatusItem()

        DispatchQueue.global(qos: .utility).async { [reader, weak self] in
            let result = Result { try reader.read() }
            DispatchQueue.main.async { [weak self] in self?.finishRefresh(result) }
        }
    }

    @discardableResult
    private func addUpdateItem(to menu: NSMenu) -> NSMenuItem {
        let item = menu.addItem(withTitle: "", action: #selector(updateApp(_:)), keyEquivalent: "")
        item.target = self
        configureUpdateItem(item)
        return item
    }

    private func configureUpdateItem(_ item: NSMenuItem) {
        item.isEnabled = !updateInProgress
        if updateInProgress {
            item.title = "Checking for Updates…"
        } else if appUpdate?.pendingApp != nil {
            item.title = "Relaunch to Update"
        } else {
            item.title = "Check for Updates…"
        }
    }

    @objc private func updateApp(_ sender: NSMenuItem) {
        guard let appUpdate, !updateInProgress else { return }
        if let path = appUpdate.pendingApp {
            prepareRelaunch(appUpdate: appUpdate, stagedApp: path)
        } else {
            checkForUpdates(manual: true)
        }
    }

    private func checkForUpdates(manual: Bool) {
        guard let appUpdate, !updateInProgress, appUpdate.beginCheck(manual: manual) else { return }
        setUpdateInProgress(true)
        DispatchQueue.global(qos: .utility).async { [weak self] in
            let result = Result { try appUpdate.check() }
            DispatchQueue.main.async { [weak self] in
                guard let self else { return }
                if case let .success(update) = result, update.state == "staged", let path = update.stagedApp {
                    appUpdate.rememberPendingApp(path)
                }
                self.setUpdateInProgress(false)
                if manual {
                    switch result {
                    case .success(let update): self.showUpdateMessage(update.message)
                    case .failure(let error): self.showUpdateMessage(error.localizedDescription)
                    }
                }
            }
        }
    }

    private func prepareRelaunch(appUpdate: AppUpdate, stagedApp: String) {
        setUpdateInProgress(true)
        DispatchQueue.global(qos: .utility).async { [weak self] in
            let result = Result { try appUpdate.startInstall(stagedApp) }
            DispatchQueue.main.async { [weak self] in
                guard let self else { return }
                switch result {
                case .success(let installer):
                    self.installer = installer
                    NSApp.terminate(nil)
                case .failure(let error):
                    self.setUpdateInProgress(false)
                    self.showUpdateMessage(error.localizedDescription)
                }
            }
        }
    }

    private func setUpdateInProgress(_ inProgress: Bool) {
        updateInProgress = inProgress
        if let updateMenuItem { configureUpdateItem(updateMenuItem) }
        updateStatusItem()
    }

    private func showUpdateMessage(_ message: String) {
        let alert = NSAlert()
        alert.messageText = "Datamak Lite Updates"
        alert.informativeText = message
        alert.addButton(withTitle: "OK")
        if let dashboardWindow, dashboardWindow.isVisible, dashboardWindow.attachedSheet == nil {
            alert.beginSheetModal(for: dashboardWindow)
        } else {
            NSApp.activate()
            alert.runModal()
        }
    }

    private func finishRefresh(_ result: Result<Snapshot, Error>) {
        state.isRefreshing = false
        switch result {
        case .success(let snapshot):
            let previous = seenSnapshot ?? snapshot
            seenSnapshot = previous
            let before = campaignContent(previous)
            let after = campaignContent(snapshot)
            changedCampaignNames = after.filter { !before.contains($0) }.map(\.name)
            changedCampaignNames += before.filter { old in !after.contains { $0.uid == old.uid } }.map(\.name)
            state.hasChanges = !changedCampaignNames.isEmpty
                || machineContent(previous) != machineContent(snapshot)
                || previous.alerts != snapshot.alerts
                || previous.hpcStatus != snapshot.hpcStatus
            state.snapshot = snapshot
            state.lastChecked = Date()
            state.error = nil
        case .failure(let error):
            state.error = error.localizedDescription
        }
        updateStatusItem()
    }

    private func campaignContent(_ snapshot: Snapshot) -> [CampaignSnapshot] {
        snapshot.campaigns.map { campaign in
            var campaign = campaign
            campaign.updatedAt = nil
            campaign.checkedAt = nil
            return campaign
        }
    }

    private func machineContent(_ snapshot: Snapshot) -> [MachineSnapshot] {
        snapshot.machines.map { machine in
            var machine = machine
            machine.updatedAt = nil
            return machine
        }
    }

    private func updateStatusItem() {
        guard let button = statusItem?.button else { return }
        let requiresAttention = state.error != nil || state.snapshot?.hasAttention == true
        let updateReady = appUpdate?.pendingApp != nil
        button.image = statusImage(requiresAttention: requiresAttention, hasChanges: state.hasChanges || updateReady)

        var tooltip = [appUpdate == nil ? "Datamak Lite · Developer" : "Datamak Lite · User"]
        if state.error != nil {
            tooltip.append("Local refresh failed. Open Datamak for details.")
        } else if let snapshot = state.snapshot {
            tooltip.append("Recorded campaigns · \(snapshot.runningCampaigns) running · \(snapshot.pendingCampaigns) pending")
            if snapshot.hasAttention {
                tooltip.append(snapshot.alerts.first?.title ?? "Some campaigns need review.")
            }
        } else {
            tooltip.append("Reading local campaign data…")
        }
        if state.hasChanges {
            let names = changedCampaignNames.prefix(2).joined(separator: ", ")
            tooltip.append(names.isEmpty ? "Datamak records changed" : "Updated: \(names)")
        }
        if updateReady {
            tooltip.append("App update ready. Right-click to relaunch.")
        }
        button.toolTip = tooltip.joined(separator: "\n")
        button.setAccessibilityLabel(tooltip.joined(separator: ". "))
    }
}

private func statusImage(requiresAttention: Bool, hasChanges: Bool) -> NSImage {
    let image = NSImage(size: NSSize(width: 26, height: 18), flipped: false) { _ in
        NSColor.black.setStroke()
        NSColor.black.setFill()

        // The torus and three pixels come from the Datamak mark.
        let torus = NSBezierPath()
        torus.move(to: NSPoint(x: 4, y: 3.4))
        torus.curve(to: NSPoint(x: 17, y: 15), controlPoint1: NSPoint(x: 0, y: 8.2),
                    controlPoint2: NSPoint(x: 8, y: 14.9))
        torus.move(to: NSPoint(x: 4, y: 3.4))
        torus.curve(to: NSPoint(x: 17, y: 6.7), controlPoint1: NSPoint(x: 7.6, y: 2.4),
                    controlPoint2: NSPoint(x: 9, y: 6.5))
        torus.move(to: NSPoint(x: 9.2, y: 8.2))
        torus.curve(to: NSPoint(x: 17, y: 11.3), controlPoint1: NSPoint(x: 11, y: 10.3),
                    controlPoint2: NSPoint(x: 13, y: 11.1))
        torus.appendOval(in: NSRect(x: 2.6, y: 3.6, width: 7.5, height: 7.5))
        torus.lineWidth = 1.8
        torus.lineCapStyle = .round
        torus.stroke()

        for dot in [NSPoint(x: 18.5, y: 14), NSPoint(x: 18.5, y: 10.4), NSPoint(x: 21.7, y: 12.2)] {
            NSBezierPath(rect: NSRect(origin: dot, size: NSSize(width: 2, height: 2))).fill()
        }
        if hasChanges {
            let ring = NSBezierPath(ovalIn: NSRect(x: 19.4, y: 1.8, width: 5.8, height: 5.8))
            ring.lineWidth = 1.4
            ring.stroke()
        }
        if requiresAttention {
            let diameter = hasChanges ? 2.7 : 4.6
            NSBezierPath(ovalIn: NSRect(x: 22.3 - diameter / 2, y: 4.7 - diameter / 2,
                                       width: diameter, height: diameter)).fill()
        }
        return true
    }
    image.isTemplate = true
    return image
}
