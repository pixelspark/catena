import Foundation
import Cryptor

/** A mutually-exclusive lock that can be used to regulate access to resources in a multithreaded environment. */
public class Mutex {
	private var mutex: pthread_mutex_t = pthread_mutex_t()

	public init() {
		var attr: pthread_mutexattr_t = pthread_mutexattr_t()
		pthread_mutexattr_init(&attr)
		#if os(Linux)
			pthread_mutexattr_settype(&attr, Int32(PTHREAD_MUTEX_RECURSIVE))		
		#else
			pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE)
		#endif

		let err = pthread_mutex_init(&self.mutex, &attr)
		pthread_mutexattr_destroy(&attr)

		switch err {
		case 0:
			// Success
			break

		case EAGAIN:
			fatalError("Could not create mutex: EAGAIN (The system temporarily lacks the resources to create another mutex.)")

		case EINVAL:
			fatalError("Could not create mutex: invalid attributes")

		case ENOMEM:
			fatalError("Could not create mutex: no memory")

		default:
			fatalError("Could not create mutex, unspecified error \(err)")
		}
	}

	private final func lock() {
		let ret = pthread_mutex_lock(&self.mutex)
		switch ret {
		case 0:
			// Success
			break

		case EDEADLK:
			fatalError("Could not lock mutex: a deadlock would have occurred")

		case EINVAL:
			fatalError("Could not lock mutex: the mutex is invalid")

		default:
			fatalError("Could not lock mutex: unspecified error \(ret)")
		}
	}

	private final func unlock() {
		let ret = pthread_mutex_unlock(&self.mutex)
		switch ret {
		case 0:
			// Success
			break

		case EPERM:
			fatalError("Could not unlock mutex: thread does not hold this mutex")

		case EINVAL:
			fatalError("Could not unlock mutex: the mutex is invalid")

		default:
			fatalError("Could not unlock mutex: unspecified error \(ret)")
		}
	}

	deinit {
		assert(pthread_mutex_trylock(&self.mutex) == 0 && pthread_mutex_unlock(&self.mutex) == 0, "deinitialization of a locked mutex results in undefined behavior!")
		pthread_mutex_destroy(&self.mutex)
	}

	/** Execute the given block while holding a lock to this mutex. */
	@discardableResult public final func locked<T>(_ file: StaticString = #file, line: UInt = #line, block: () throws -> (T)) rethrows -> T {
		self.lock()
		defer {
			self.unlock()
		}
		let ret: T = try block()
		return ret
	}
}

enum Fallible<T> {
	case success(T)
	case failure(String)
}

struct POSIXError: Error {
	let code: Int32
	let file: String
	let line: Int
	let column: Int
	let function: String

	init(code: Int32? = nil, file: String = #file, line: Int = #line, column: Int = #column, function: String = #function) {
		self.code = code ?? errno
		self.file = file
		self.line = line
		self.column = column
		self.function = function
	}
}

public func posix(_ block: @autoclosure () -> Int32, file: String = #file, line: Int = #line, column: Int = #column, function: String = #function) throws {
	guard block() == 0 else {
		throw POSIXError(file: file, line: line, column: column, function: function)
	}
}

internal func random<T: ExpressibleByIntegerLiteral> (_ type: T.Type) -> T {
	var r: T = 0
	let bytes = try! Random.generate(byteCount: MemoryLayout<T>.size)
	memcpy(&r, bytes, MemoryLayout<T>.size)
	return r
}

extension Data {
	var sha256: Data {
		return Data(bytes: Digest(using: .sha256).update(data: self)!.final())
	}
}

#if os(Linux)
@discardableResult internal func autoreleasepool<T>(_ block: () throws -> (T)) rethrows -> T {
	return try block()
}
#endif

extension Date {
	/**	Returns an ISO-8601 formatted string of this date, in the locally preferred timezone. Should only be used for
	presentational purposes. */
	public var iso8601FormattedLocalDate: String {
		let dateFormatter = DateFormatter()
		dateFormatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
		return dateFormatter.string(from: self)
	}

	/** Returns an ISO-8601 formatted string representation of this date, in the UTC timezone ('Zulu time', that's why it
	ends in 'Z'). */
	public var iso8601FormattedUTCDate: String {
		let formatter = DateFormatter()
		formatter.timeZone = TimeZone(abbreviation: "UTC")
		formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"
		return formatter.string(from: self)
	}

	public var unixTime: Double {
		return self.timeIntervalSince1970
	}
}

extension URL {
	var parameters: [String: String] {
		if let uc = URLComponents(url: self, resolvingAgainstBaseURL: false), let items = uc.queryItems {
			var values: [String: String] = [:]

			for item in items {
				if let v = item.value {
					values[item.name] = v
				}
			}
			return values
		}
		else {
			return [:]
		}
	}
}

internal extension Array where Element == Double {
	var median: Double {
		let sortedArray = sorted()
		if count % 2 != 0 {
			return Double(sortedArray[count / 2])
		} else {
			return Double(sortedArray[count / 2] + sortedArray[count / 2 - 1]) / 2.0
		}
	}
}

extension String {
	public var hexDecoded: Data? {
		var error = false
		let s = Array(self.characters)
		let numbers = stride(from: 0, to: s.count, by: 2).map() { (idx: Int) -> UInt8 in
			let res = strtoul(String(s[idx ..< Swift.min(idx + 2, s.count)]), nil, 16)
			if res > UInt(UInt8.max) {
				error = true
				return UInt8(0)
			}
			return UInt8(res)
		}

		if error {
			return nil
		}

		return Data(bytes: numbers)
	}
}


extension UInt8 {
	var numberOfLeadingZeroBits: Int {
		var n = 0
		var b = self
		if b == 0 {
			n += 8
		}
		else {
			while (b & 0x80) == 0 {
				b <<= 1
				n += 1
			}
		}
		return n
	}
}

extension Data {
	public mutating func appendRaw<T>(_ item: T) {
		var item = item
		let ptr = withUnsafePointer(to: &item) { ptr in
			return UnsafeRawPointer(ptr)
		}
		self.append(ptr.assumingMemoryBound(to: UInt8.self), count: MemoryLayout<T>.size)
	}
}
