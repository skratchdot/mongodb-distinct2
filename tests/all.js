/*global db: true, assert: true, startParallelShell: true */
/*jslint indent: 4, plusplus: true */
(function () {
	'use strict';

	var t = db.jstests_all, undef, res, count, i, j, p;
	t.drop();

	/************************************************************/
	// Empty collections
	assert.eq([], t.distinct2('a')); // as string
	assert.eq([], t.distinct2(['a'])); // as array
	assert.eq([], t.distinct2(['a'], false)); // as array, no count
	assert.eq([], t.distinct2(['a'], true)); // as array, with count
	assert.eq([], t.distinct2('b')); // non-existent key
	assert.eq(0, t.find().count());

	// db.distinct() === db.distinct2()
	assert.eq(t.distinct('a'), t.distinct2('a')); // as string
	assert.eq(t.distinct('b'), t.distinct2('b')); // non-existent key


	/************************************************************/
	// 1 Item
	t.save({ a : 123 });
	assert.eq([123], t.distinct2('a')); // as string
	assert.eq([123], t.distinct2(['a'])); // as array
	assert.eq([123], t.distinct2(['a'], false)); // as array, no count
	assert.eq([[123, 1]], t.distinct2(['a'], true)); // as array, with count
	assert.eq([], t.distinct2('b')); // non-existent key
	assert.eq(1, t.find().count());

	// db.distinct() === db.distinct2()
	assert.eq(t.distinct('a'), t.distinct2('a')); // as string
	assert.eq(t.distinct('b'), t.distinct2('b')); // non-existent key


	/************************************************************/
	// 2 of the same value
	t.save({ a : 123 });
	assert.eq([123], t.distinct2(['a'])); // as array
	assert.eq([[123, 2]], t.distinct2(['a'], true)); // as array, with count
	assert.eq(2, t.find().count());

	// db.distinct() === db.distinct2()
	assert.eq(t.distinct('a'), t.distinct2('a')); // as string
	assert.eq(t.distinct('b'), t.distinct2('b')); // non-existent key


	/************************************************************/
	// Multiple types
	t.save({ a : 'wow' });
	assert.eq([123, 'wow'], t.distinct2('a'));
	assert.eq([[123, 2], ['wow', 1]], t.distinct2('a', true));
	assert.eq(3, t.find().count());

	// db.distinct() === db.distinct2()
	assert.eq(t.distinct('a'), t.distinct2('a')); // as string
	assert.eq(t.distinct('b'), t.distinct2('b')); // non-existent key


	/************************************************************/
	// Nested keys
	t.save({ a : { b : 321 } });
	assert.eq([321], t.distinct2('a.b'));
	assert.eq(4, t.find().count());

	t.save({ a : { c : 654 } });
	assert.eq([654], t.distinct2('a.c'));
	assert.eq(5, t.find().count());

	// db.distinct() === db.distinct2()
	assert.eq(t.distinct('a'), t.distinct2('a')); // as string
	assert.eq(t.distinct('b'), t.distinct2('b')); // non-existent key


	/************************************************************/
	// Ordering
	assert.eq([[321, undef], [undef, 654]], t.distinct2(['a.b', 'a.c'])); // no counts
	assert.eq([[undef, 321], [654, undef]], t.distinct2(['a.c', 'a.b'])); // no counts
	assert.eq([[321, undef, 1], [undef, 654, 1]], t.distinct2(['a.b', 'a.c'], true)); // with counts
	assert.eq([[undef, 321, 1], [654, undef, 1]], t.distinct2(['a.c', 'a.b'], true)); // with counts


	/************************************************************/
	// mongodb test suite: distinct1.js
	t.drop();

	assert.eq(0, t.distinct2("a").length, "test empty");

	t.save({ a : 1 });
	t.save({ a : 2 });
	t.save({ a : 2 });
	t.save({ a : 2 });
	t.save({ a : 3 });


	res = t.distinct2("a");
	assert.eq("1,2,3", res.toString(), "A1");

	//assert.eq("1,2", t.distinct2("a", { a : { $lt : 3 } }), "A2");

	t.drop();

	t.save({ a : { b : "a" }, c : 12 });
	t.save({ a : { b : "b" }, c : 12 });
	t.save({ a : { b : "c" }, c : 12 });
	t.save({ a : { b : "c" }, c : 12 });

	res = t.distinct2("a.b");
	assert.eq("a,b,c", res.toString(), "B1");
	//assert.eq("BasicCursor", t._distinct2("a.b").stats.cursor, "B2")

	/************************************************************/
	// mongodb test suite: distinct2.js
	t.drop();

	t.save({a : null});
	assert.eq(0, t.distinct2('a.b').length, "A");

	t.drop();
	t.save({ a : 1 });
	assert.eq([1], t.distinct2("a"), "B");
	t.save({});
	assert.eq([1], t.distinct2("a"), "C");


	/************************************************************/
	// mongodb test suite: distinct1.js
	// Yield and delete test case for query optimizer cursor.  SERVER-4401
	t.drop();

	t.ensureIndex({a : 1});
	t.ensureIndex({b : 1});

	for (i = 0; i < 50; ++i) {
	    for (j = 0; j < 20; ++j) {
	        t.save({a : i, c : i, d : j});
	    }
	}
	for (i = 0; i < 1000; ++i) {
	    t.save({b : i, c : i + 50});
	}
	db.getLastError();

	// The idea here is to try and remove the last match for the {a:1} index scan while distinct is yielding.
	p = startParallelShell('for(i = 0; i < 2500; ++i) { db.jstests_distinct3.remove({a:49}); for(j = 0; j < 20; ++j) { db.jstests_distinct3.save({a:49, c:49, d:j}) } }');

	for (i = 0; i < 100; ++i) {
	    count = t.distinct2('c', {$or : [{a : {$gte : 0}, d : 0}, {b : {$gte : 0}}]}).length;
	    assert.gt(count, 1000);
	}

	p();


	/************************************************************/
	// End of test suite. Make sure to drop our test collection
	t.drop();

}());