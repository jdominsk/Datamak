import Foundation

struct AppUpdateResult: Decodable, Sendable {
    let state: String
    let message: String
    let stagedApp: String?
}

struct AppUpdate: Sendable {
    private static let interval: TimeInterval = 24 * 60 * 60
    private static let lastCheckKey = "DatamakLastUpdateCheck"
    private static let pendingAppKey = "DatamakPendingUpdate"

    private let appURL: URL
    private let helper: URL?
    private let arguments: [String]
    private let python: String
    private let configDirectory: String?

    init?(bundle: Bundle, python: String, liteRoot: String, configDirectory: String?, opensWindow: Bool) {
        guard bundle.object(forInfoDictionaryKey: "DatamakRole") as? String == "user",
              bundle.object(forInfoDictionaryKey: "DatamakDemo") as? Bool != true else { return nil }

        appURL = bundle.bundleURL
        helper = bundle.url(forResource: "app_update", withExtension: "py")
        self.python = python
        self.configDirectory = configDirectory
        var arguments = ["--app", appURL.path, "--lite-root", liteRoot, "--python", python]
        if let revision = bundle.object(forInfoDictionaryKey: "DatamakSourceRevision") as? String {
            arguments += ["--revision", revision]
        }
        if opensWindow { arguments.append("--window") }
        self.arguments = arguments
    }

    func beginCheck(manual: Bool, now: Date = Date(), defaults: UserDefaults = .standard) -> Bool {
        if !manual, let lastCheck = defaults.object(forKey: Self.lastCheckKey) as? Date,
           now.timeIntervalSince(lastCheck) < Self.interval {
            return false
        }
        defaults.set(now, forKey: Self.lastCheckKey)
        return true
    }

    var pendingApp: String? {
        guard let path = UserDefaults.standard.string(forKey: Self.pendingAppKey),
              FileManager.default.fileExists(atPath: path) else { return nil }
        return path
    }

    func rememberPendingApp(_ path: String) {
        UserDefaults.standard.set(path, forKey: Self.pendingAppKey)
    }

    func check() throws -> AppUpdateResult {
        let process = try makeProcess()
        let output = Pipe()
        process.standardOutput = output
        try process.run()
        let data = output.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else { throw UpdateError.checkFailed }

        return try decodeResult(data)
    }

    func startInstall(_ path: String) throws -> Process {
        let process = try makeProcess(extraArguments: [
            "--apply", path, "--wait-pid", String(ProcessInfo.processInfo.processIdentifier),
        ])
        let output = Pipe()
        process.standardOutput = output
        try process.run()

        // The helper reports readiness while this app is still running. Waiting
        // for it to exit here would deadlock because it waits for this app.
        var data = Data()
        while data.count < 16_384,
              let chunk = try output.fileHandleForReading.read(upToCount: 4_096), !chunk.isEmpty {
            data.append(chunk)
            if let newline = data.firstIndex(of: 0x0a) {
                let response = try decodeResult(data.prefix(upTo: newline))
                guard response.state == "ready" else { throw UpdateError.installFailed(response.message) }
                return process
            }
        }
        throw UpdateError.installFailed("Datamak could not prepare the update. The app is still running.")
    }

    private func decodeResult(_ data: Data) throws -> AppUpdateResult {
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        guard let result = try? decoder.decode(AppUpdateResult.self, from: data) else {
            throw UpdateError.checkFailed
        }
        return result
    }

    private func makeProcess(extraArguments: [String] = []) throws -> Process {
        guard let helper else { throw UpdateError.missingHelper }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: python)
        process.arguments = ["-B", helper.path] + arguments + extraArguments
        process.standardInput = FileHandle.nullDevice
        process.standardOutput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice
        var environment = ProcessInfo.processInfo.environment
        if let configDirectory { environment["DATAMAK_HOME"] = configDirectory }
        process.environment = environment
        return process
    }

    private enum UpdateError: LocalizedError {
        case missingHelper
        case checkFailed
        case installFailed(String)

        var errorDescription: String? {
            switch self {
            case .missingHelper: "This build is missing its Datamak update helper."
            case .checkFailed: "Datamak could not check for app updates."
            case .installFailed(let message): message
            }
        }
    }
}
