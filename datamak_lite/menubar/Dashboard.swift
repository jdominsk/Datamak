import AppKit
import SwiftUI
import UniformTypeIdentifiers

private typealias DashboardState<Value> = SwiftUI.State<Value>

struct Snapshot: Decodable, Equatable {
    let generatedAt: String
    let mode: String
    let campaigns: [CampaignSnapshot]
    let machines: [MachineSnapshot]
    let alerts: [SnapshotAlert]
    let hpcStatus: String

    var runningCampaigns: Int { campaigns.filter { ($0.running ?? 0) > 0 }.count }
    var pendingCampaigns: Int { campaigns.filter { ($0.pending ?? 0) > 0 }.count }
    var hasAttention: Bool { !alerts.isEmpty || campaigns.contains { $0.needsReview } }
}

struct CampaignSnapshot: Decodable, Equatable, Identifiable {
    let uid: String
    let name: String
    let profilePath: String?
    let databasePath: String?
    var updatedAt: String?
    var checkedAt: String?
    let machines: [String]
    let running: Int?
    let pending: Int?
    let completed: Int?
    let attention: Int?
    let countKind: String?
    let state: String
    let message: String?

    var id: String { uid }
    var needsReview: Bool { state == "attention" || state == "unavailable" || (attention ?? 0) > 0 }
}

struct MachineSnapshot: Decodable, Equatable, Identifiable {
    let id: String
    let name: String
    let remoteRuns: Int
    let archivedRuns: Int
    let knownBytes: Int64?
    let measuredRuns: Int
    let quotaUsedBytes: Int64?
    let quotaLimitBytes: Int64?
    var updatedAt: String?
    let storageScope: String?
    let storage: [StorageRecord]?
}

struct StorageRecord: Decodable, Equatable, Identifiable {
    let id: String
    let name: String
    let path: String
    let kind: String
    let sizeBytes: Int64?
    let observedAt: String?
    let archivePath: String?
    let note: String?
    let archiveState: String?
    let archiveDetail: String?
    let archivePlanEligible: Bool?
}

struct SnapshotAlert: Decodable, Equatable, Identifiable {
    let id: String
    let title: String
    let detail: String
    let campaignUid: String?
}

struct Dashboard: View {
    static let defaultSize = NSSize(width: 468, height: 564)

    let snapshot: Snapshot?
    let error: String?
    let isRefreshing: Bool
    let isDemo: Bool
    let lastChecked: Date?
    let hasChanges: Bool
    let onRefresh: () -> Void
    let onHide: () -> Void
    @ObservedObject var archiveQueue: ArchiveQueue
    let archiveQueueReadError: String?
    @ObservedObject var remoteQuotas: RemoteQuotaStore
    var resizesVertically = false

    @DashboardState private var tab: Tab = .campaigns
    @DashboardState private var filter: CampaignFilter = .all
    @DashboardState private var detail: Detail?
    @DashboardState private var storageFilter: StorageFilter = .remote
    @DashboardState private var archiveActionError: String?
    @DashboardState private var archiveExportMessage: String?
    @Environment(\.colorScheme) private var scheme

    private enum Tab: String, CaseIterable { case campaigns = "Campaigns", storage = "Storage" }
    private enum Detail: Hashable { case campaign(String), machine(String), archiveQueue }
    private enum CampaignFilter: String { case all, running, pending, review }
    private enum StorageFilter: String, CaseIterable {
        case remote, archive, disk

        var title: String {
            switch self {
            case .remote: "Remote runs"
            case .archive: "Archives"
            case .disk: "Measurements"
            }
        }
    }
    private enum ArchiveBadge: String, CaseIterable {
        case unarchived, queued, archived, review

        var title: String {
            switch self {
            case .unarchived: "Unarchived"
            case .queued: "To be archived"
            case .archived: "Archived"
            case .review: "Archive needs review"
            }
        }

        var symbol: String {
            switch self {
            case .unarchived: "circle.dashed"
            case .queued: "flag.fill"
            case .archived: "checkmark.circle.fill"
            case .review: "clock.arrow.circlepath"
            }
        }
    }

    private var accent: Color {
        scheme == .dark ? Color(red: 0.44, green: 0.83, blue: 0.72) :
            Color(red: 0.08, green: 0.39, blue: 0.33)
    }
    private var amber: Color {
        scheme == .dark ? Color(red: 1, green: 0.77, blue: 0.36) :
            Color(red: 0.56, green: 0.32, blue: 0.05)
    }
    private var blue: Color {
        scheme == .dark ? Color(red: 0.53, green: 0.74, blue: 0.98) :
            Color(red: 0.21, green: 0.34, blue: 0.58)
    }

    var body: some View {
        VStack(spacing: 0) {
            header
            if let snapshot {
                if error != nil {
                    Label("Refresh failed. Showing the previous snapshot.", systemImage: "exclamationmark.circle")
                        .font(.caption)
                        .foregroundStyle(amber)
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .padding(.horizontal, 20)
                        .padding(.bottom, 8)
                        .help(error ?? "")
                }
                if let detail {
                    ScrollView {
                        LazyVStack(alignment: .leading, spacing: 15) {
                            Button {
                                self.detail = nil
                            } label: {
                                Label(tab.rawValue, systemImage: "chevron.left")
                            }
                            .buttonStyle(.plain)
                            .font(.system(size: 12, weight: .semibold))
                            .foregroundStyle(accent)
                            detailContent(detail, in: snapshot)
                        }
                        .padding(.horizontal, 20)
                        .padding(.vertical, 12)
                    }
                    .id(detail)
                } else {
                    Picker("View", selection: $tab) {
                        ForEach(Tab.allCases, id: \.self) { Text($0.rawValue).tag($0) }
                    }
                    .pickerStyle(.segmented)
                    .labelsHidden()
                    .controlSize(.large)
                    .padding(.horizontal, 20)
                    .padding(.bottom, 8)

                    ScrollView {
                        VStack(alignment: .leading, spacing: 15) {
                            switch tab {
                            case .campaigns: campaignsView(snapshot)
                            case .storage: storageView(snapshot)
                            }
                        }
                        .padding(.horizontal, 20)
                        .padding(.top, 5)
                        .padding(.bottom, 14)
                    }
                    .id(tab)
                }
            } else {
                VStack(spacing: 9) {
                    if let error {
                        Image(systemName: "exclamationmark.circle")
                            .font(.title)
                            .foregroundStyle(amber)
                        Text("Couldn’t load campaigns").font(.headline)
                        Text(error).font(.callout).foregroundStyle(.secondary)
                            .multilineTextAlignment(.center)
                            .textSelection(.enabled)
                        Button("Try again", action: onRefresh)
                            .disabled(isRefreshing)
                    } else {
                        ProgressView().controlSize(.small)
                        Text("Reading campaign records…").font(.callout).foregroundStyle(.secondary)
                    }
                }
                .padding(24)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
            footer
        }
        .frame(width: Self.defaultSize.width,
               height: resizesVertically ? nil : Self.defaultSize.height)
        .frame(minHeight: resizesVertically ? Self.defaultSize.height : nil,
               maxHeight: resizesVertically ? .infinity : nil)
        .tint(accent)
        .alert("Archive queue", isPresented: Binding(
            get: { archiveActionError != nil },
            set: { if !$0 { archiveActionError = nil } }
        )) {
            Button("OK", role: .cancel) { archiveActionError = nil }
        } message: {
            Text(archiveActionError ?? "")
        }
    }

    private var header: some View {
        HStack(spacing: 11) {
            ZStack {
                RoundedRectangle(cornerRadius: 11)
                    .fill(accent.opacity(0.13))
                    .frame(width: 41, height: 41)
                Image(systemName: "cylinder.split.1x2")
                    .font(.system(size: 20, weight: .medium))
                    .foregroundStyle(accent)
            }
            .accessibilityHidden(true)
            VStack(alignment: .leading, spacing: 2) {
                Text("Datamak Lite")
                    .font(.system(size: 21, weight: .semibold))
                Text(isDemo ? "Example campaign data" : "Local campaign records")
                    .font(.system(size: 11))
                    .foregroundStyle(.secondary)
            }
            Spacer()
            if isRefreshing {
                ProgressView()
                    .controlSize(.small)
                    .frame(width: 30, height: 30)
                    .accessibilityLabel("Refreshing")
            } else {
                Button(action: onRefresh) {
                    Image(systemName: "arrow.clockwise")
                        .font(.system(size: 15, weight: .medium))
                        .frame(width: 30, height: 30)
                        .contentShape(Rectangle())
                }
                .buttonStyle(.borderless)
                .help("Refresh local records")
                .accessibilityLabel("Refresh local records")
                .keyboardShortcut("r", modifiers: .command)
            }
        }
        .padding(.horizontal, 20)
        .padding(.top, 17)
        .padding(.bottom, 14)
    }

    private var footer: some View {
        VStack(spacing: 0) {
            Divider()
            HStack(spacing: 6) {
                if hasChanges {
                    Circle().fill(blue).frame(width: 6, height: 6)
                        .accessibilityHidden(true)
                    Text("New changes").foregroundStyle(blue)
                    Text("·").accessibilityHidden(true)
                }
                if let lastChecked {
                    Text("Local check \(lastChecked.formatted(date: .omitted, time: .shortened))")
                        .help(lastChecked.formatted(date: .complete, time: .standard))
                }
                Spacer()
                if let snapshot, !isDemo {
                    Text(snapshot.hpcStatus == "cached" ? "Recorded HPC status" : "HPC unchecked")
                        .help("Campaign activity comes from Datamak records. This app checks the local registry.")
                    Text("·").accessibilityHidden(true)
                }
                Button("Hide", action: onHide)
                    .buttonStyle(.borderless)
                    .help("Hide the panel and keep Datamak running")
            }
            .font(.system(size: 11))
            .foregroundStyle(.secondary)
            .padding(.horizontal, 20)
            .frame(height: 35)
        }
    }

    private func campaignsView(_ snapshot: Snapshot) -> some View {
        let alerts = snapshot.alerts
        let visibleAlerts = filter == .review ? alerts : Array(alerts.prefix(2))
        let campaigns = snapshot.campaigns
        let reviewCount = campaigns.filter { needsReview($0, in: alerts) }.count
        let visible = campaigns.filter { campaign in
            switch filter {
            case .all: true
            case .running: (campaign.running ?? 0) > 0
            case .pending: (campaign.pending ?? 0) > 0
            case .review: needsReview(campaign, in: alerts)
            }
        }
        return VStack(alignment: .leading, spacing: 13) {
            HStack(spacing: 8) {
                counter("Running", snapshot.runningCampaigns, icon: "play.circle", color: accent, filter: .running)
                counter("Pending", snapshot.pendingCampaigns, icon: "clock", color: blue, filter: .pending)
                counter("To review", reviewCount, icon: "exclamationmark.circle", color: amber, filter: .review)
            }
            .accessibilityElement(children: .contain)
            .accessibilityLabel("Campaign counts")

            if !alerts.isEmpty && filter != .running && filter != .pending {
                VStack(alignment: .leading, spacing: 7) {
                    HStack {
                        sectionTitle("Needs attention")
                        Spacer()
                        if visibleAlerts.count < alerts.count {
                            Button("See all \(alerts.count)") { filter = .review }
                                .font(.caption)
                                .buttonStyle(.borderless)
                        }
                    }
                    VStack(spacing: 0) {
                        ForEach(visibleAlerts) { alert in
                            if alert.id != visibleAlerts.first?.id { rowDivider }
                            if let campaign = campaigns.first(where: { $0.uid == alert.campaignUid }) {
                                Button { detail = .campaign(campaign.uid) } label: {
                                    alertRow(alert, campaignName: campaign.name)
                                }
                                    .buttonStyle(.plain)
                            } else {
                                alertRow(alert)
                            }
                        }
                    }
                    .background(amber.opacity(0.08), in: RoundedRectangle(cornerRadius: 10))
                }
            }

            VStack(alignment: .leading, spacing: 7) {
                HStack {
                    sectionTitle(filter == .all ? "Registered campaigns" : "\(visible.count) of \(campaigns.count) campaigns")
                    Spacer()
                    if filter == .all {
                        Text(campaigns.count.formatted())
                            .monospacedDigit()
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    } else {
                        Button("Show all") { filter = .all }
                            .font(.caption)
                            .buttonStyle(.borderless)
                    }
                }
                if visible.isEmpty {
                    Text(campaigns.isEmpty ? "No Datamak Lite campaigns are registered." : "No campaigns in this view.")
                        .font(.callout)
                        .foregroundStyle(.secondary)
                        .padding(12)
                } else {
                    VStack(spacing: 0) {
                        ForEach(visible) { campaign in
                            if campaign.id != visible.first?.id { rowDivider }
                            Button {
                                detail = .campaign(campaign.uid)
                            } label: {
                                campaignRow(campaign)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .background(panelFill, in: RoundedRectangle(cornerRadius: 10))
                }
            }
        }
    }

    private func storageView(_ snapshot: Snapshot) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                sectionTitle("Storage by machine")
                Spacer()
                archiveQueueLink
            }
            if snapshot.machines.isEmpty {
                Text("No machine storage records are available.")
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .padding(12)
            }
            ForEach(snapshot.machines) { machine in
                machineCard(machine)
            }
            Text(isDemo ? "Illustrative sizes and quotas." : "Sizes and run locations come from recorded observations.")
                .font(.caption)
                .foregroundStyle(.secondary)
                .padding(.top, 2)
        }
    }

    @ViewBuilder
    private func detailContent(_ detail: Detail, in snapshot: Snapshot) -> some View {
        switch detail {
        case .campaign(let uid):
            if let campaign = snapshot.campaigns.first(where: { $0.uid == uid }) {
                VStack(alignment: .leading, spacing: 7) {
                    Text(campaign.name).font(.title2.weight(.semibold))
                    statusPill(campaign.state)
                }
                let alerts = snapshot.alerts.filter { $0.campaignUid == uid }
                ForEach(alerts) { alert in
                    alertRow(alert)
                        .background(amber.opacity(0.08), in: RoundedRectangle(cornerRadius: 10))
                }
                VStack(alignment: .leading, spacing: 9) {
                    sectionTitle("Recorded \(campaign.countKind ?? "activity")")
                    detailLine("Running", campaign.running?.formatted() ?? "Unknown")
                    detailLine("Pending", campaign.pending?.formatted() ?? "Unknown")
                    detailLine("Completed", campaign.completed?.formatted() ?? "Unknown")
                    if let count = campaign.attention, count > 0 {
                        detailLine("Recorded issues", count.formatted())
                    }
                    detailLine("Machine", campaign.machines.isEmpty ? "Unspecified" : campaign.machines.joined(separator: ", "))
                    if let date = displayDate(campaign.updatedAt) { detailLine("Last record update", date) }
                    if let date = displayDate(campaign.checkedAt) { detailLine("Last status observation", date) }
                }
                .padding(12)
                .background(panelFill, in: RoundedRectangle(cornerRadius: 10))
                if alerts.isEmpty, let message = campaign.message {
                    Text(message).font(.callout).foregroundStyle(.secondary)
                }
                if let path = campaign.databasePath { pathLine("Campaign database", path) }
                if let path = campaign.profilePath { pathLine("Campaign profile", path) }
            } else {
                Text("This campaign is no longer in the registry.").foregroundStyle(.secondary)
            }
        case .machine(let id):
            if let machine = snapshot.machines.first(where: { $0.id == id }) {
                HStack {
                    Text(machine.name).font(.title2.weight(.semibold))
                    quotaRefreshButton(machine)
                    Spacer()
                    archiveQueueLink
                }
                machineDetails(machine)
                storageRecords(machine)
            } else {
                Text("This machine is no longer in the snapshot.").foregroundStyle(.secondary)
            }
        case .archiveQueue:
            archiveQueueView(snapshot)
        }
    }

    private func counter(_ name: String, _ value: Int, icon: String, color: Color, filter target: CampaignFilter) -> some View {
        let selected = filter == target
        return Button {
            filter = selected ? .all : target
        } label: {
            VStack(alignment: .leading, spacing: 4) {
                HStack(spacing: 5) {
                    Image(systemName: icon).foregroundStyle(color)
                    Text(name).foregroundStyle(.secondary)
                }
                .font(.system(size: 11, weight: .medium))
                Text(value.formatted())
                    .font(.system(size: 25, weight: .semibold, design: .rounded))
                    .monospacedDigit()
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.horizontal, 11)
            .padding(.vertical, 8)
            .background(color.opacity(selected ? 0.18 : 0.085), in: RoundedRectangle(cornerRadius: 10))
            .overlay(RoundedRectangle(cornerRadius: 10).strokeBorder(color.opacity(selected ? 0.5 : 0), lineWidth: 1))
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .accessibilityLabel("\(name) campaigns")
        .accessibilityValue(value.formatted())
        .accessibilityAddTraits(selected ? .isSelected : [])
    }

    private func campaignRow(_ campaign: CampaignSnapshot) -> some View {
        HStack(spacing: 8) {
            VStack(alignment: .leading, spacing: 3) {
                Text(campaign.name)
                    .font(.system(size: 12, weight: .medium))
                    .lineLimit(2)
                    .multilineTextAlignment(.leading)
                Text(activity(campaign))
                    .font(.system(size: 11))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Spacer(minLength: 4)
            statusPill(campaign.state)
            chevron
        }
        .padding(.horizontal, 11)
        .padding(.vertical, 8)
        .contentShape(Rectangle())
    }

    private func alertRow(_ alert: SnapshotAlert, campaignName: String? = nil) -> some View {
        HStack(alignment: .top, spacing: 8) {
            Image(systemName: "exclamationmark.circle.fill")
                .font(.system(size: 13))
                .foregroundStyle(amber)
                .accessibilityHidden(true)
            VStack(alignment: .leading, spacing: 3) {
                Text(campaignName ?? alert.title).font(.system(size: 12, weight: .semibold))
                Text(campaignName == nil ? alert.detail : "\(alert.title) · \(alert.detail)")
                    .font(.system(size: 11))
                    .foregroundStyle(.secondary)
                    .lineLimit(campaignName == nil ? nil : 2)
            }
            .multilineTextAlignment(.leading)
            .frame(maxWidth: .infinity, alignment: .leading)
            if campaignName != nil { chevron.padding(.top, 3) }
        }
        .padding(10)
        .contentShape(Rectangle())
    }

    private func machineCard(_ machine: MachineSnapshot) -> some View {
        VStack(alignment: .leading, spacing: 5) {
            HStack(spacing: 8) {
                Button {
                    storageFilter = .remote
                    detail = .machine(machine.id)
                } label: {
                    HStack(spacing: 6) {
                        Image(systemName: "externaldrive")
                            .foregroundStyle(accent)
                            .accessibilityHidden(true)
                        Text(machine.name).font(.system(size: 13, weight: .semibold))
                        chevron
                    }
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .accessibilityLabel("Open \(machine.name) storage")
                quotaRefreshButton(machine)
                Spacer()
            }
            Text("\(machine.remoteRuns.formatted()) remote runs · \(machine.archivedRuns.formatted()) archive entries")
                .font(.system(size: 11))
                .foregroundStyle(.secondary)
            quotaReadings(machine, compact: true)
            quotaFeedback(machine)
        }
        .padding(11)
        .background(panelFill, in: RoundedRectangle(cornerRadius: 10))
    }

    private func machineDetails(_ machine: MachineSnapshot) -> some View {
        VStack(alignment: .leading, spacing: 9) {
            sectionTitle(machine.storageScope.map { "\($0) storage" } ?? "Recorded storage")
            detailLine("Remote runs", machine.remoteRuns.formatted())
            detailLine("Archive entries", machine.archivedRuns.formatted())
            detailLine("Recorded disk usage", machine.knownBytes.map(bytes) ?? "Unknown")
            if machine.knownBytes != nil, let date = displayDate(machine.updatedAt) {
                detailLine("Disk measurement recorded", date)
            }
            quotaReadings(machine, compact: false)
            quotaFeedback(machine)
        }
        .padding(12)
        .background(panelFill, in: RoundedRectangle(cornerRadius: 10))
    }

    @ViewBuilder
    private func quotaRefreshButton(_ machine: MachineSnapshot) -> some View {
        if !isDemo && remoteQuotas.supports(machineID: machine.id) {
            if remoteQuotas.refreshing.contains(machine.id) {
                ProgressView()
                    .controlSize(.small)
                    .frame(width: 22, height: 22)
                    .accessibilityLabel("Checking \(machine.name) quota")
            } else {
                Button {
                    remoteQuotas.refresh(machineID: machine.id)
                } label: {
                    Image(systemName: "arrow.clockwise").frame(width: 22, height: 22)
                }
                .buttonStyle(.borderless)
                .help("Refresh \(machine.name) quota")
                .accessibilityLabel("Refresh \(machine.name) quota")
            }
        }
    }

    @ViewBuilder
    private func quotaReadings(_ machine: MachineSnapshot, compact: Bool) -> some View {
        let remote = isDemo ? nil : remoteQuotas.results[machine.id]
        if let remote, !remote.quotas.isEmpty {
            let quotas = compact ? Array(remote.quotas.prefix(1)) : remote.quotas
            ForEach(Array(quotas.enumerated()), id: \.offset) { _, quota in
                quotaReading(scope: quota.scope, used: quota.usedBytes, limit: quota.limitBytes)
            }
            HStack(spacing: 8) {
                if let date = displayDate(remote.observedAt, includeTime: true) {
                    Text("Quota checked \(date)")
                }
                if compact && remote.quotas.count > 1 {
                    Text("\(remote.quotas.count - 1) more in details")
                }
            }
            .font(.caption2)
            .foregroundStyle(.secondary)
        } else if let used = machine.quotaUsedBytes, let limit = machine.quotaLimitBytes, limit > 0 {
            quotaReading(scope: machine.storageScope ?? "Quota", used: used, limit: limit)
            if let date = displayDate(machine.updatedAt) {
                Text("Quota recorded \(date)").font(.caption2).foregroundStyle(.secondary)
            }
        } else {
            Text("Quota unknown").font(.system(size: 11)).foregroundStyle(.secondary)
        }
    }

    private func quotaReading(scope: String, used: Int64, limit: Int64) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            if limit > 0 {
                detailLine(scope, "\(bytes(used)) of \(bytes(limit))")
                quotaBar(used: used, limit: limit)
            } else {
                detailLine(scope, "\(bytes(used)) used · quota limit unavailable")
            }
        }
    }

    @ViewBuilder
    private func quotaFeedback(_ machine: MachineSnapshot) -> some View {
        if !isDemo, let result = remoteQuotas.results[machine.id], result.state != "ok" {
            HStack(alignment: .firstTextBaseline, spacing: 8) {
                Text(result.message)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                    .frame(maxWidth: .infinity, alignment: .leading)
                if result.state == "connect_required" {
                    Button("Connect…") {
                        remoteQuotas.connect(machineID: machine.id)
                    }
                    .buttonStyle(.borderless)
                    .help("Open Terminal to connect to \(machine.name)")
                    .accessibilityLabel("Connect to \(machine.name)")
                    .disabled(remoteQuotas.refreshing.contains(machine.id))
                }
            }
        }
    }

    private func quotaBar(used: Int64, limit: Int64) -> some View {
        let fraction = Double(used) / Double(limit)
        return ProgressView(value: min(1, max(0, fraction)))
            .controlSize(.small)
            .tint(fraction >= 0.9 ? amber : accent)
            .accessibilityLabel("Quota usage")
            .accessibilityValue(fraction.formatted(.percent.precision(.fractionLength(0))))
    }

    private func storageRecords(_ machine: MachineSnapshot) -> some View {
        let records = (machine.storage ?? []).filter { $0.kind == storageFilter.rawValue }
        return VStack(alignment: .leading, spacing: 8) {
            Picker("Storage records", selection: $storageFilter) {
                ForEach(StorageFilter.allCases, id: \.self) { Text($0.title).tag($0) }
            }
            .labelsHidden()
            .pickerStyle(.segmented)
            if storageFilter == .remote && !records.isEmpty {
                HStack(spacing: 9) {
                    ForEach(ArchiveBadge.allCases, id: \.self) { badge in
                        Label(badge == .review ? "Review" : badge.title, systemImage: badge.symbol)
                            .foregroundStyle(archiveColor(badge))
                    }
                }
                .font(.system(size: 10))
                .accessibilityElement(children: .contain)
                .accessibilityLabel("Archive status legend")
            }
            if records.isEmpty {
                Text("No \(storageFilter.title.lowercased()) recorded.")
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .padding(8)
            } else {
                LazyVStack(alignment: .leading, spacing: 0) {
                    ForEach(records) { record in
                        if record.id != records.first?.id { rowDivider }
                        storageRecordRow(record, machine: machine)
                        .padding(.horizontal, 11)
                        .padding(.vertical, 8)
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .background(panelFill, in: RoundedRectangle(cornerRadius: 10))
            }
        }
    }

    private func storageRecordRow(_ record: StorageRecord, machine: MachineSnapshot) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack(spacing: 5) {
                if record.kind == "remote" {
                    let entry = ArchiveQueueEntry(machineID: machine.id, machineName: machine.name,
                                                  path: record.path, name: record.name)
                    runArchiveButton(record, entry: entry)
                }
                Text(record.name).font(.system(size: 12, weight: .medium))
            }
            storagePath(record.path)
            if record.kind != "remote", let path = record.archivePath { storagePath(path) }
            if let size = record.sizeBytes {
                Text([bytes(size), displayDate(record.observedAt)].compactMap { $0 }.joined(separator: " · "))
                    .font(.caption)
            } else if let date = displayDate(record.observedAt) {
                Text(date).font(.caption)
            }
            if let note = record.kind == "remote" ? nil : record.note {
                Text(note).font(.caption).foregroundStyle(.secondary)
            }
        }
    }

    private func storagePath(_ path: String) -> some View {
        Text(path)
            .font(.system(size: 10, design: .monospaced))
            .foregroundStyle(.secondary)
            .textSelection(.enabled)
    }

    private func archiveColor(_ badge: ArchiveBadge) -> Color {
        switch badge {
        case .unarchived: Color.secondary
        case .queued: blue
        case .archived: accent
        case .review: amber
        }
    }

    private func runArchiveButton(_ record: StorageRecord, entry: ArchiveQueueEntry) -> some View {
        let stored = ArchiveBadge(rawValue: record.archiveState ?? "") ?? .unarchived
        let flagged = archiveQueue.contains(machineID: entry.machineID, path: entry.path)
        let badge = flagged && stored != .archived ? ArchiveBadge.queued : stored
        let action = flagged ? "Remove from archive queue" : "Flag for archiving"
        let icon = Image(systemName: badge.symbol)
            .font(.system(size: 13))
            .foregroundStyle(archiveColor(badge))
            .frame(width: 18, height: 18)
            .contentShape(Rectangle())

        return Group {
            if stored == .archived && !flagged {
                icon.help(record.archiveDetail ?? badge.title)
                    .accessibilityLabel(badge.title)
            } else {
                Button {
                    setArchiveFlag(!flagged, entry: entry)
                } label: {
                    icon
                }
                .buttonStyle(.plain)
                .help([badge.title, record.archiveDetail, action].compactMap { $0 }.joined(separator: "\n"))
                .accessibilityLabel("\(action) for \(record.name)")
                .accessibilityValue(badge.title)
            }
        }
    }

    private var archiveQueueLink: some View {
        Button {
            archiveExportMessage = nil
            detail = .archiveQueue
        } label: {
            Label("Archive queue (\(archiveQueue.entries.count))", systemImage: "tray")
                .font(.system(size: 11, weight: .medium))
        }
        .buttonStyle(.borderless)
    }

    private func archiveQueueView(_ snapshot: Snapshot) -> some View {
        let entries = archiveQueue.entries
        let runs = Dictionary(uniqueKeysWithValues: snapshot.machines.flatMap { machine in
            (machine.storage ?? []).filter { $0.kind == "remote" }.map { record in
                (ArchiveQueueEntry(machineID: machine.id, machineName: machine.name,
                                   path: record.path, name: record.name).id, record)
            }
        })
        let gxPaths = entries.compactMap { entry -> String? in
            guard let run = runs[entry.id], run.archivePlanEligible == true, run.archiveState != "archived" else {
                return nil
            }
            return entry.path
        }
        return VStack(alignment: .leading, spacing: 9) {
            Text("Archive queue").font(.title2.weight(.semibold))
            if let archiveQueueReadError {
                Text(archiveQueueReadError).font(.caption).foregroundStyle(amber)
            }
            if entries.isEmpty {
                Text("Flag a run under Storage → Remote runs to add it here.")
                    .font(.callout)
                    .foregroundStyle(.secondary)
            } else {
                if !isDemo {
                    HStack {
                        Text("\(gxPaths.count) for Perlmutter GX planning").font(.caption)
                            .foregroundStyle(.secondary)
                        Spacer()
                        Button("Export GX queue…") { exportArchiveQueue(gxPaths) }
                            .disabled(gxPaths.isEmpty || archiveQueueReadError != nil)
                    }
                }
                if let archiveExportMessage {
                    Text(archiveExportMessage).font(.caption).foregroundStyle(.secondary)
                }
                LazyVStack(alignment: .leading, spacing: 0) {
                    ForEach(entries) { entry in
                        if entry.id != entries.first?.id { rowDivider }
                        HStack(alignment: .top, spacing: 7) {
                            VStack(alignment: .leading, spacing: 4) {
                                Text(entry.name).font(.system(size: 12, weight: .medium))
                                storagePath(entry.path)
                                Text(archiveQueueDescription(entry, run: runs[entry.id]))
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            Spacer(minLength: 2)
                            Button {
                                setArchiveFlag(false, entry: entry)
                            } label: {
                                Image(systemName: "minus.circle").frame(width: 20, height: 20)
                            }
                            .buttonStyle(.borderless)
                            .help("Remove from archive queue")
                            .accessibilityLabel("Remove \(entry.name) from archive queue")
                        }
                        .padding(9)
                    }
                }
                .background(panelFill, in: RoundedRectangle(cornerRadius: 10))
            }
        }
    }

    private func archiveQueueDescription(_ entry: ArchiveQueueEntry, run: StorageRecord?) -> String {
        guard let run else { return "\(entry.machineName) · Run record unavailable" }
        if run.archiveState == "archived" { return "\(entry.machineName) · Archived" }
        if run.archivePlanEligible == true { return "\(entry.machineName) · Perlmutter GX planning" }
        return "\(entry.machineName) · Outside Perlmutter GX planning"
    }

    private func setArchiveFlag(_ flagged: Bool, entry: ArchiveQueueEntry) {
        do {
            try archiveQueue.setFlagged(flagged, entry: entry)
            archiveExportMessage = nil
        } catch {
            archiveActionError = error.localizedDescription
        }
    }

    private func exportArchiveQueue(_ paths: [String]) {
        do {
            let data = try gxPlanningList(paths: paths)
            let panel = NSSavePanel()
            panel.allowedContentTypes = [.plainText]
            panel.nameFieldStringValue = "gx-archive-queue.txt"
            panel.prompt = "Export"
            guard panel.runModal() == .OK, let url = panel.url else { return }
            try data.write(to: url, options: .atomic)
            archiveExportMessage = "Exported \(paths.count) GX paths for archive planning."
        } catch {
            archiveActionError = error.localizedDescription
        }
    }

    private func needsReview(_ campaign: CampaignSnapshot, in alerts: [SnapshotAlert]) -> Bool {
        campaign.needsReview || alerts.contains { $0.campaignUid == campaign.uid }
    }

    private func activity(_ campaign: CampaignSnapshot) -> String {
        var parts = campaign.machines
        if let running = campaign.running, running > 0 { parts.append("\(running) running") }
        if let pending = campaign.pending, pending > 0 { parts.append("\(pending) pending") }
        if parts.isEmpty {
            if campaign.state == "unavailable" { return "Campaign records unavailable" }
            if let completed = campaign.completed, completed > 0 { return "\(completed) completed" }
            return "Activity unrecorded"
        }
        return parts.joined(separator: " · ")
    }

    private func statusPill(_ state: String) -> some View {
        let label: String
        let color: Color
        switch state {
        case "running": (label, color) = ("Running", accent)
        case "pending": (label, color) = ("Pending", blue)
        case "attention": (label, color) = ("Review", amber)
        case "unavailable": (label, color) = ("Unavailable", Color.secondary)
        case "idle": (label, color) = ("Idle", Color.secondary)
        default: (label, color) = ("Unknown", Color.secondary)
        }
        return Text(label)
            .font(.system(size: 10, weight: .medium))
            .foregroundStyle(color)
            .padding(.horizontal, 7)
            .padding(.vertical, 3)
            .background(color.opacity(0.1), in: Capsule())
    }

    private var panelFill: Color {
        scheme == .dark ? .white.opacity(0.055) : .black.opacity(0.035)
    }

    private var rowDivider: some View { Divider().padding(.horizontal, 11) }
    private var chevron: some View {
        Image(systemName: "chevron.right")
            .font(.system(size: 9, weight: .semibold))
            .foregroundStyle(.tertiary)
            .accessibilityHidden(true)
    }

    private func sectionTitle(_ text: String) -> some View {
        Text(text).font(.system(size: 12, weight: .semibold)).accessibilityAddTraits(.isHeader)
    }

    private func detailLine(_ key: String, _ value: String) -> some View {
        HStack(alignment: .firstTextBaseline) {
            Text(key).foregroundStyle(.secondary)
            Spacer(minLength: 12)
            Text(value).multilineTextAlignment(.trailing).textSelection(.enabled)
        }
        .font(.system(size: 12))
    }

    private func pathLine(_ title: String, _ path: String) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            sectionTitle(title)
            Text(path)
                .font(.system(size: 11, design: .monospaced))
                .foregroundStyle(.secondary)
                .textSelection(.enabled)
        }
    }

    private func bytes(_ value: Int64) -> String {
        let units: [UnitInformationStorage] = [.tebibytes, .gibibytes, .mebibytes, .kibibytes, .bytes]
        let measurement = Measurement(value: Double(value), unit: UnitInformationStorage.bytes)
        let unit = units.first { measurement.converted(to: $0).value >= 1 } ?? .bytes
        return measurement.converted(to: unit).formatted(
            .measurement(width: .abbreviated, usage: .asProvided, numberFormatStyle: .number.precision(.fractionLength(0...2)))
        )
    }

    private func displayDate(_ raw: String?, includeTime: Bool = false) -> String? {
        guard let raw, !raw.isEmpty else { return nil }
        let formatter = ISO8601DateFormatter()
        let date = formatter.date(from: raw)
        formatter.formatOptions.insert(.withFractionalSeconds)
        return (date ?? formatter.date(from: raw))?.formatted(
            date: .abbreviated, time: includeTime ? .shortened : .omitted
        ) ?? raw
    }
}
