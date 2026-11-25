import Combine
import CryptoKit
import Foundation

struct ArchiveQueueEntry: Codable, Equatable, Identifiable {
    let machineID: String
    let machineName: String
    let path: String
    let name: String

    var id: String {
        Self.id(machineID: machineID, path: path)
    }

    fileprivate static func id(machineID: String, path: String) -> String {
        let identity = Data((machineID + "\u{0}" + path).utf8)
        return SHA256.hash(data: identity).map { String(format: "%02x", $0) }.joined()
    }
}

enum ArchiveQueueError: LocalizedError {
    case invalidPath(String)
    case invalidMachine
    case mismatchedEntry(String)
    case emptyPlanningList

    var errorDescription: String? {
        switch self {
        case .invalidPath(let path):
            return "The archive queue requires a normalized absolute directory path. Check \(String(reflecting: path))."
        case .invalidMachine:
            return "The archive queue entry needs a machine identifier."
        case .mismatchedEntry(let file):
            return "The archive queue entry in \(file) does not match its filename."
        case .emptyPlanningList:
            return "Choose at least one run for GX archive planning."
        }
    }
}

@MainActor
final class ArchiveQueue: ObservableObject {
    @Published private(set) var entries: [ArchiveQueueEntry]
    private let directory: URL?
    private let files = FileManager.default

    init(isDemo: Bool = false, directory: URL? = nil) {
        if isDemo {
            self.directory = nil
            entries = [ArchiveQueueEntry(machineID: "perlmutter", machineName: "Perlmutter",
                                         path: "/scratch/demo/gx/replay/queued", name: "Queued replay")]
        } else {
            self.directory = directory ?? URL.applicationSupportDirectory
                .appending(path: "Datamak Lite", directoryHint: .isDirectory)
                .appending(path: "Archive Queue", directoryHint: .isDirectory)
            entries = []
        }
    }

    func reload() throws {
        guard let directory else { return }
        let urls: [URL]
        do {
            urls = try files.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil,
                                                 options: [.skipsHiddenFiles])
        } catch where Self.isMissing(error) {
            entries = []
            return
        }

        let decoder = JSONDecoder()
        var loaded: [ArchiveQueueEntry] = []
        for url in urls where url.pathExtension == "json" {
            do {
                let entry = try decoder.decode(ArchiveQueueEntry.self, from: Data(contentsOf: url))
                try Self.validate(entry)
                guard url.deletingPathExtension().lastPathComponent == entry.id else {
                    throw ArchiveQueueError.mismatchedEntry(url.lastPathComponent)
                }
                loaded.append(entry)
            } catch where Self.isMissing(error) {
                // Another running dashboard can remove its flag during a refresh.
                continue
            }
        }
        let sorted = Self.sorted(loaded)
        if entries != sorted { entries = sorted }
    }

    func contains(machineID: String, path: String) -> Bool {
        let id = ArchiveQueueEntry.id(machineID: machineID, path: path)
        return entries.contains { $0.id == id }
    }

    func setFlagged(_ flagged: Bool, entry: ArchiveQueueEntry) throws {
        try Self.validate(entry)
        if let directory {
            let url = directory.appending(path: entry.id + ".json", directoryHint: .notDirectory)
            if flagged {
                try files.createDirectory(at: directory, withIntermediateDirectories: true)
                let data = try JSONEncoder().encode(entry)
                try data.write(to: url, options: .atomic)
            } else {
                do {
                    try files.removeItem(at: url)
                } catch where Self.isMissing(error) {
                    // Removing an already cleared flag leaves the same state.
                }
            }
        }

        var updated = entries.filter { $0.id != entry.id }
        if flagged { updated.append(entry) }
        entries = Self.sorted(updated)
    }

    private static func validate(_ entry: ArchiveQueueEntry) throws {
        guard !entry.machineID.isEmpty, !entry.machineID.contains("\u{0}") else {
            throw ArchiveQueueError.invalidMachine
        }
        try validateQueuePath(entry.path)
    }

    private static func sorted(_ entries: [ArchiveQueueEntry]) -> [ArchiveQueueEntry] {
        entries.sorted { first, second in
            if first.machineName != second.machineName { return first.machineName < second.machineName }
            if first.name != second.name { return first.name < second.name }
            return first.path.utf8.lexicographicallyPrecedes(second.path.utf8)
        }
    }

    private static func isMissing(_ error: Error) -> Bool {
        let code = (error as? CocoaError)?.code
        return code == .fileNoSuchFile || code == .fileReadNoSuchFile
    }
}

/// The GX planner accepts one exact directory per line with `--queue-file`.
func gxPlanningList(paths: [String]) throws -> Data {
    guard !paths.isEmpty else { throw ArchiveQueueError.emptyPlanningList }
    let records = try paths.map { path in
        try validateQueuePath(path)
        return Data(path.utf8)
    }
    var text = Data()
    for record in Set(records).sorted(by: { $0.lexicographicallyPrecedes($1) }) {
        text.append(record)
        text.append(0x0A)
    }
    return text
}

private func validateQueuePath(_ path: String) throws {
    let separators = CharacterSet.controlCharacters.union(.newlines)
    let components = path.split(separator: "/", omittingEmptySubsequences: false).dropFirst()
    guard path.hasPrefix("/"),
          path == path.trimmingCharacters(in: .whitespacesAndNewlines),
          path.rangeOfCharacter(from: separators) == nil,
          components.allSatisfy({ !$0.isEmpty && $0 != "." && $0 != ".." }) else {
        throw ArchiveQueueError.invalidPath(path)
    }
}
