import XCTest
@testable import CatenaTests
@testable import CatenaSQLTests

XCTMain([
    testCase(CatenaTests.allTests),
    testCase(CatenaSQLTests.allTests),
])
