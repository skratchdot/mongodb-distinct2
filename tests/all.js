(function () {
	var t = db.jstests_all;
	var undef;
	t.drop();

	// Empty collections
	assert.eq( [], t.distinct2('a') ); // as string
	assert.eq( [], t.distinct2(['a']) ); // as array
	assert.eq( [], t.distinct2(['a'], false) ); // as array, no count
	assert.eq( [], t.distinct2(['a'], true) ); // as array, with count
	assert.eq( [], t.distinct2('b') ); // non-existent key
	assert.eq( 0, t.find().count() );

	// 1 Item
	t.save( { a : 123 } );
	assert.eq( [123], t.distinct2('a') ); // as string
	assert.eq( [123], t.distinct2(['a']) ); // as array
	assert.eq( [123], t.distinct2(['a'], false) ); // as array, no count
	assert.eq( [[123,1]], t.distinct2(['a'], true) ); // as array, with count
	assert.eq( [], t.distinct2('b') ); // non-existent key
	assert.eq( 1, t.find().count() );

	// 2 of the same value
	t.save( { a : 123 } );
	assert.eq( [123], t.distinct2(['a']) ); // as array
	assert.eq( [[123,2]], t.distinct2(['a'], true) ); // as array, with count
	assert.eq( 2, t.find().count() );

	// Multiple types
	t.save( { a : 'wow' } );
	assert.eq( [123,'wow'], t.distinct2('a') );
	assert.eq( [[123,2],['wow',1]], t.distinct2('a', true) );
	assert.eq( 3, t.find().count() );
	
	// Nested keys
	t.save( { a : { b : 321 } } );
	assert.eq( [321], t.distinct2('a.b') );
	assert.eq( 4, t.find().count() );
	t.save( { a : { c : 654 } } );
	assert.eq( [654], t.distinct2('a.c') );
	assert.eq( 5, t.find().count() );

	// Ordering
	assert.eq( [[321,undef],[undef,654]], t.distinct2(['a.b','a.c']) ); // no counts
	assert.eq( [[undef,321],[654,undef]], t.distinct2(['a.c','a.b']) ); // no counts
	assert.eq( [[321,undef,1],[undef,654,1]], t.distinct2(['a.b','a.c'], true) ); // with counts
	assert.eq( [[undef,321,1],[654,undef,1]], t.distinct2(['a.c','a.b'], true) ); // with counts

	t.drop();
}());