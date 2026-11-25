import AppKit
import Combine

struct RemoteQuota: Codable {
    let scope: String
    let usedBytes: Int64
    let limitBytes: Int64
}

struct RemoteQuotaResult: Codable {
    let state: String
    let message: String
    let sshHost: String?
    let connectArgs: [String]?
    let observedAt: String?
    let quotas: [RemoteQuota]
}

@MainActor
final class RemoteQuotaStore: ObservableObject {
    @Published private(set) var results: [String: RemoteQuotaResult] = [:]
    @Published private(set) var refreshing: Set<String> = []

    private static let loginHosts = [
        "polaris": "polaris.alcf.anl.gov",
        "aurora": "aurora.alcf.anl.gov",
        "perlmutter": "perlmutter.nersc.gov",
    ]
    private let script: URL?
    private let python: String
    private let defaults: UserDefaults
    private let enabled: Bool

    init(bundle: Bundle, python: String, enabled: Bool,
         defaults: UserDefaults = UserDefaults(suiteName: "org.datamak.lite.remote-quotas") ?? .standard) {
        self.python = python
        self.enabled = enabled
        self.defaults = defaults
        script = bundle.url(forResource: "remote_quota", withExtension: "py")
        guard enabled else { return }
        for machineID in Self.loginHosts.keys {
            if let data = defaults.data(forKey: machineID),
               let cached = try? JSONDecoder().decode(RemoteQuotaResult.self, from: data) {
                results[machineID] = cached
            }
        }
    }

    func supports(machineID: String) -> Bool {
        enabled && Self.loginHosts[machineID] != nil
    }

    func refresh(machineID: String) {
        guard supports(machineID: machineID), refreshing.insert(machineID).inserted else { return }
        let script = self.script
        let python = self.python
        DispatchQueue.global(qos: .utility).async { [weak self] in
            let outcome = Result { try Self.read(script: script, python: python, machineID: machineID) }
            DispatchQueue.main.async { [weak self] in
                self?.finish(machineID: machineID, outcome: outcome)
            }
        }
    }

    func connect(machineID: String) {
        guard enabled, let host = Self.loginHosts[machineID],
              let previous = results[machineID], previous.state == "connect_required" else { return }
        // Use only the known facility login or its local SSH alias.
        let destination = previous.sshHost == machineID ? machineID : host
        let dedicated = previous.connectArgs == ["-M", "-o", "ControlPath=~/.ssh/datamak-%C", destination]
        let command = dedicated ? "ssh -M -o ControlPath=~/.ssh/datamak-%C \(destination)" : "ssh \(destination)"
        let message = openConnection(to: destination, dedicated: dedicated)
            ? "Finish signing in, then use Refresh to check the quota."
            : "Open Terminal and run \(command), then use Refresh."
        results[machineID] = RemoteQuotaResult(state: previous.state, message: message,
                                               sshHost: destination, connectArgs: previous.connectArgs,
                                               observedAt: previous.observedAt,
                                               quotas: previous.quotas)
    }

    private func openConnection(to destination: String, dedicated: Bool) -> Bool {
        if !dedicated {
            return URL(string: "ssh://\(destination)").map { NSWorkspace.shared.open($0) } ?? false
        }

        let files = FileManager.default
        let launcher = files.temporaryDirectory.appendingPathComponent("Datamak Login \(UUID()).command")
        let command = """
        #!/bin/sh
        /bin/rm -f -- "$0"
        exec /usr/bin/ssh -M -o 'ControlPath=~/.ssh/datamak-%C' \(destination)
        """
        do {
            try Data(command.utf8).write(to: launcher, options: .atomic)
            try files.setAttributes([.posixPermissions: 0o700], ofItemAtPath: launcher.path)
            if NSWorkspace.shared.open(launcher) { return true }
        } catch {}
        try? files.removeItem(at: launcher)
        return false
    }

    private func finish(machineID: String, outcome: Result<RemoteQuotaResult, Error>) {
        refreshing.remove(machineID)
        let result: RemoteQuotaResult
        switch outcome {
        case .success(let response):
            result = response
        case .failure(let error):
            result = RemoteQuotaResult(state: "unavailable", message: error.localizedDescription,
                                       sshHost: nil, connectArgs: nil, observedAt: nil, quotas: [])
        }

        if result.state == "ok", let data = try? JSONEncoder().encode(result) {
            defaults.set(data, forKey: machineID)
        }
        let previous = results[machineID]
        results[machineID] = RemoteQuotaResult(
            state: result.state, message: result.message, sshHost: result.sshHost, connectArgs: result.connectArgs,
            observedAt: result.observedAt ?? previous?.observedAt,
            quotas: result.state == "ok" ? result.quotas : previous?.quotas ?? []
        )
    }

    private nonisolated static func read(script: URL?, python: String, machineID: String) throws -> RemoteQuotaResult {
        guard let script else { throw QuotaError.helperUnavailable }
        let process = Process()
        process.executableURL = URL(fileURLWithPath: python)
        process.arguments = ["-B", script.path, "--machine", machineID]
        let output = Pipe()
        process.standardInput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice
        process.standardOutput = output
        try process.run()
        let data = output.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else { throw QuotaError.refreshFailed }
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        guard let result = try? decoder.decode(RemoteQuotaResult.self, from: data) else {
            throw QuotaError.refreshFailed
        }
        return result
    }

    private enum QuotaError: LocalizedError {
        case helperUnavailable
        case refreshFailed

        var errorDescription: String? {
            switch self {
            case .helperUnavailable: "This build is missing the remote quota helper."
            case .refreshFailed: "Datamak could not read the quota response."
            }
        }
    }
}
