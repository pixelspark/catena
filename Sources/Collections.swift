/* Copyright (c) 2014-2016 Pixelspark, Tommy van der Vorst

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated 
documentation files (the "Software"), to deal in the Software without restriction, including without limitation the 
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit 
persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the 
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE 
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR 
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR 
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. */
import Foundation

public struct OrderedDictionaryGenerator<KeyType: Hashable, ValueType>: IteratorProtocol {
	public typealias Element = (KeyType, ValueType)
	private let orderedDictionary: OrderedDictionary<KeyType, ValueType>
	private var keyGenerator: IndexingIterator<[KeyType]>

	init(orderedDictionary: OrderedDictionary<KeyType, ValueType>) {
		self.orderedDictionary = orderedDictionary
		self.keyGenerator = self.orderedDictionary.keys.makeIterator()
	}

	mutating public func next() -> Element? {
		if let nextKey = self.keyGenerator.next() {
			return (nextKey, self.orderedDictionary.values[nextKey]!)
		}
		return nil
	}
}

public struct OrderedDictionary<KeyType: Hashable, ValueType>: Sequence {
	public typealias KeyArrayType = [KeyType]
	public typealias DictionaryType = [KeyType: ValueType]
	public typealias Iterator = OrderedDictionaryGenerator<KeyType, ValueType>
	public typealias PairType = (key: KeyType, value: ValueType)

	public private(set) var keys = KeyArrayType()
	public private(set) var values = DictionaryType()

	public init() {
		// Empty ordered dictionary
	}

	public init(dictionaryInAnyOrder: DictionaryType) {
		self.values = dictionaryInAnyOrder
		self.keys = [KeyType](dictionaryInAnyOrder.keys)
	}

	public func makeIterator() -> Iterator {
		return OrderedDictionaryGenerator(orderedDictionary: self)
	}

	public var count: Int {
		return keys.count
	}

	public mutating func remove(_ key: KeyType) {
		keys.remove(at: keys.index(of: key)!)
		values.removeValue(forKey: key)
	}

	public mutating func insert(_ value: ValueType, forKey key: KeyType, atIndex index: Int) -> ValueType? {
		var adjustedIndex = index
		let existingValue = self.values[key]
		if existingValue != nil {
			let existingIndex = self.keys.index(of: key)!

			if existingIndex < index {
				adjustedIndex -= 1
			}
			self.keys.remove(at: existingIndex)
		}

		self.keys.insert(key, at:adjustedIndex)
		self.values[key] = value
		return existingValue
	}

	public func contains(_ key: KeyType) -> Bool {
		return self.values[key] != nil
	}

	/** Keeps only the keys present in the 'keys' parameter and puts them in the specified order. The 'keys' parameter is
	not allowed to contain keys that do not exist in the ordered dictionary, or contain the same key twice. */
	public mutating func filterAndOrder(_ keyOrder: [KeyType]) {
		var newKeySet = Set<KeyType>()
		for k in keyOrder {
			if contains(k) {
				if newKeySet.contains(k) {
					precondition(false, "Key appears twice in specified key order")
				}
				else {
					newKeySet.insert(k)
				}
			}
			else {
				precondition(false, "Key '\(k)' does not exist in the current ordered dictionary and can't be ordered")
			}
		}

		// Remove keys that weren't ordered
		for k in self.keys {
			if !newKeySet.contains(k) {
				self.values.removeValue(forKey: k)
			}
		}
		self.keys = keyOrder
	}

	public mutating func orderKey(_ key: KeyType, toIndex: Int) {
		precondition(self.keys.index(of: key) != nil, "key to be ordered must exist")
		self.keys.remove(at: self.keys.index(of: key)!)
		self.keys.insert(contentsOf: [key], at: toIndex)
	}

	public mutating func orderKey(_ key: KeyType, beforeKey: KeyType) {
		if let newIndex = self.keys.index(of: beforeKey) {
			orderKey(key, toIndex: newIndex)
		}
		else {
			precondition(false, "key to order before must exist")
		}
	}

	public mutating func removeAtIndex(_ index: Int) -> (KeyType, ValueType)
	{
		precondition(index < self.keys.count, "Index out-of-bounds")
		let key = self.keys.remove(at: index)
		let value = self.values.removeValue(forKey: key)!
		return (key, value)
	}

	public mutating func append(_ value: ValueType, forKey: KeyType) {
		precondition(!contains(forKey), "Ordered dictionary already contains value")
		self.keys.append(forKey)
		self.values[forKey] = value
	}

	public mutating func replaceOrAppend(_ value: ValueType, forKey key: KeyType) {
		if !contains(key) {
			self.keys.append(key)
		}
		self.values[key] = value
	}

	public mutating func sortKeysInPlace(_ isOrderedBefore: (_ a: KeyType, _ b: KeyType) -> Bool) {
		self.keys.sort(by: isOrderedBefore)
	}

	public mutating func sortPairsInPlace(_ isOrderedBefore: (PairType, PairType) -> Bool) {
		self.keys.sort { a, b in
			return isOrderedBefore((a, self.values[a]!), (b, self.values[b]!))
		}
	}

	public subscript(key: KeyType) -> ValueType? {
		get {
			return self.values[key]
		}
		set {
			if let n = newValue {
				self.replaceOrAppend(n, forKey: key)
			}
			else {
				self.remove(key)
			}
		}
	}

	public subscript(index: Int) -> (KeyType, ValueType) {
		get {
			precondition(index < self.keys.count, "Index out-of-bounds")
			let key = self.keys[index]
			let value = self.values[key]!
			return (key, value)
		}
	}
}

extension OrderedSet : CustomStringConvertible, CustomDebugStringConvertible {
	/** A textual representation of `self`. */
	public var description: String {
		return array.description
	}

	/** A textual representation of `self`, suitable for debugging. */
	public var debugDescription: String {
		return array.debugDescription
	}
}

/** OrderdSet is an ordered collection of unique `Element` instances.  Adapted from OrderedSet.swift, created by
Bradley Hilton, used under MIT License. Copyright (c) 2014 Skyvive <brad@skyvive.com>.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the 
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the 
Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE 
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR 
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR 
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. */
public struct OrderedSet<Element : Hashable> : Hashable, Collection, MutableCollection {
	internal(set) var array: [Element]
	internal(set) var set: Set<Element>

	/** Always zero, which is the index of the first element when non-empty. **/
	public var startIndex: Int {
		return array.startIndex
	}

	/** A "past-the-end" element index; the successor of the last valid subscript argument. **/
	public var endIndex: Int {
		return array.endIndex
	}

	public subscript(position: Int) -> Element {
		get {
			return array[position]
		}
		set {
			let oldValue = array[position]
			set.remove(oldValue)
			array[position] = newValue
			set.insert(newValue)
			array = array.enumerated().filter { (arg) -> Bool in let (index, element) = arg; return index == position || element.hashValue != newValue.hashValue }.map { $0.element }
		}
	}

	public var hashValue: Int {
		return set.hashValue
	}

	public func index(after i: Int) -> Int {
		return i + 1
	}

	internal static func collapse<Element : Hashable, S : Sequence>(_ s: S) -> ([Element], Set<Element>) where S.Iterator.Element == Element {
		var aSet = Set<Element>()
		return (s.filter { item in
			if aSet.contains(item) {
				return false
			}
			aSet.insert(item)
			return true
		}, aSet)
	}
}

public func ==<T : Hashable>(lhs: OrderedSet<T>, rhs: OrderedSet<T>) -> Bool {
	return lhs.set == rhs.set
}

/** Array-related functions for OrderedSet. */
extension OrderedSet : ExpressibleByArrayLiteral, RangeReplaceableCollection {
	/** Create an instance containing `elements`. */
	public init(arrayLiteral elements: Element...) {
		(self.array, self.set) = OrderedSet.collapse(elements)
	}

	/** Construct an empty OrderedSet. */
	public init() {
		self.array = []
		self.set = []
	}

	/** Construct from an arbitrary sequence with elements of type `Element`. */
	public init<S : Sequence>(_ s: S) where S.Iterator.Element == Element {
		(self.array, self.set) = OrderedSet.collapse(s)
	}

	public mutating func replaceSubrange<C>(_ subrange: Range<Int>, with newElements: C) where C : Collection, C.Iterator.Element == Element {
		let oldArray = array[subrange]
		let oldSet = Set(oldArray)
		let (newArray, newSet) = OrderedSet.collapse(newElements)
		let deletions = oldSet.subtracting(newSet)
		set.subtract(deletions)
		set.formUnion(newSet)
		array.replaceSubrange(subrange, with: newArray)
		array = array.enumerated().filter { (index, element) in return subrange.contains(index) || subrange.lowerBound == index || !newSet.contains(element) }.map { $0.element }
	}

	public var capacity: Int {
		return array.capacity
	}


	/** If `!self.isEmpty`, remove the last element and return it, otherwise return `nil`. */
	public mutating func popLast() -> Element! {
		guard let last = array.popLast() else { return nil }
		set.remove(last)
		return last
	}

}

/** Operator form of `appendContentsOf`. */
public func +=<Element, S : Sequence>( lhs: inout OrderedSet<Element>, rhs: S) where S.Iterator.Element == Element {
	lhs.append(contentsOf: rhs)
}

/** Set-related functions for OrderedSet. */
public extension OrderedSet {
	public init(minimumCapacity: Int) {
		self.array = []
		self.set = Set(minimumCapacity: minimumCapacity)
		reserveCapacity(minimumCapacity)
	}

	/** Returns `true` if the ordered set contains a member. */
	public func contains(member: Element) -> Bool {
		return set.contains(member)
	}

	/** Remove the member from the ordered set and return it if it was present. */
	@discardableResult public mutating func remove(_ member: Element) -> Element? {
		guard let index = array.index(of: member) else { return nil }
		set.remove(member)
		return array.remove(at: index)
	}

	/** Returns true if the ordered set is a subset of a finite sequence as a `Set`. */
	public func isSubset(of otherSet: Set<Element>) -> Bool {
		return set.isSubset(of: otherSet)
	}

	/** Returns true if the ordered set is a subset of a finite sequence as a `Set` but not equal. */
	public func isStrictSubset(of otherSet: Set<Element>) -> Bool {
		return set.isStrictSubset(of: otherSet)
	}

	/** Returns true if the ordered set is a superset of a finite sequence as a `Set`. */
	public func isSuperset(of otherSet: Set<Element>) -> Bool {
		return set.isSuperset(of: otherSet)
	}

	/** Returns true if the ordered set is a superset of a finite sequence as a `Set` but not equal. */
	public func isStrictSuperset(of otherSet: Set<Element>) -> Bool {
		return set.isStrictSuperset(of: otherSet)
	}

	/** Returns true if no members in the ordered set are in a finite sequence as a `Set`. */
	public func isDisjoint(with otherSet: Set<Element>) -> Bool {
		return set.isDisjoint(with: otherSet)
	}

	/** Return a new `OrderedSet` with items in both this set and a finite sequence. */
	public func union(with otherSet: OrderedSet<Element>) -> OrderedSet {
		var c = self
		c.formUnion(with: otherSet)
		return c
	}

	/** Append elements of a finite sequence into this `OrderedSet`. */
	public mutating func formUnion(with otherSet: OrderedSet<Element>) {
		append(contentsOf: otherSet)
	}

	/** Return a new ordered set with elements in this set that do not occur in a finite sequence. */
	public func subtracting(_ otherSet: Set<Element>) -> OrderedSet {
		var c = self
		c.subtract(otherSet)
		return c
	}

	/** Remove all members in the ordered set that occur in a finite sequence. */
	public mutating func subtract(_ otherSet: Set<Element>) {
		set.subtract(otherSet)
		array = array.filter { set.contains($0) }
	}

	/** Return a new ordered set with elements common to this ordered set and a finite sequence. */
	public func intersection(with otherSet: Set<Element>) -> OrderedSet {
		var c = self
		c.formIntersection(with: otherSet)
		return c
	}

	/** Remove any members of this ordered set that aren't also in a finite sequence. */
	public mutating func formIntersection(with otherSet: Set<Element>) {
		set.formIntersection(otherSet)
		array = array.filter { set.contains($0) }
	}

	/** If `!self.isEmpty`, remove the first element and return it, otherwise return `nil`. */
	public mutating func popFirst() -> Element? {
		guard let first = array.first else { return nil }
		set.remove(first)
		return array.removeFirst()
	}
}
