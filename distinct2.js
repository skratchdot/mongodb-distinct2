/*global DBCollection: false, DBQuery: false */
/*jslint indent: 4, plusplus: true */
/**
 * MongoDB - distinct2.js
 * 
 *      Version: 1.0
 *         Date: August 15, 2012
 *      Project: http://skratchdot.com/projects/mongodb-distinct2/
 *  Source Code: https://github.com/skratchdot/mongodb-distinct2/
 *       Issues: https://github.com/skratchdot/mongodb-distinct2/issues/
 * Dependencies: MongoDB v1.8+
 * 
 * Description:
 * 
 * Similar to the db.myCollection.distinct() function, distinct2() allows
 * you to pass in an array of keys to get values for.  It also allows you
 * to pass in an optional "count" argument.  When true, you can easily get
 * the counts for your distinct values.
 * 
 * You can also call distinct2() on the results of a query (something you
 * can't currently do with the built in distinct() function).
 * 
 * To accomplish this, it adds the following functions to our built in mongo prototypes:  
 * 
 *     DBCollection.prototype.distinct2 = function (keys, count) {};
 *     DBQuery.prototype.distinct2 = function (keys, count) {};
 *
 * Usage:
 * 
 * // All 4 of these statements are the same as:
 * //
 * //     db.users.distinct('name.first')
 * //
 * db.users.distinct2('name.first');
 * db.users.distinct2('name.first', false);
 * db.users.distinct2(['name.first']);
 * db.users.distinct2(['name.first'], false);
 * 
 * // you can pass in an array of values
 * db.users.distinct2(['name.first','name.last']);
 * 
 * // you can get counts
 * db.users.distinct2('name.first', true);
 * 
 * // you can get distinct values from the results of a query
 * db.users.find({'name.first':'Bob'}).distinct('name.last');
 * 
 * 
 * 
 * 
 */
(function () {
	'use strict';

	var isArray, getFromKeyString;

	/**
	 * Same behavior as Array.isArray()
	 * @function
	 * @name isArray
	 * @private
	 * @param obj {*} The object to test
	 * @returns {boolean} Will return true of obj is an array, otherwise will return false
	 */
	isArray = function (obj) {
		return Object.prototype.toString.call(obj) === '[object Array]';
	};

	/**
	 * @function
	 * @name getFromKeyString
	 * @private
	 * @param obj {object} The object to search
	 * @param keyString {string} A dot delimited string path
	 * @returns {object|undefined} If we find the path provide by keyString, we will return the value at that path
	 */
	getFromKeyString = function (obj, keyString) {
		var arr, i;
		if (typeof keyString === 'string') {
			arr = keyString.split('.');
			for (i = 0; i < arr.length; i++) {
				if (typeof obj === 'object' && obj.hasOwnProperty(arr[i])) {
					obj = obj[arr[i]];
				} else {
					return;
				}
			}
			return obj;
		}
	};

	/**
	 * @function
	 * @name distinct2
	 * @memberOf DBQuery
	 * @param keys {string|array} The array of dot delimited keys to get the distinct values for
	 * @param count {boolean} Whether or not to append
	 * @returns {array} If keys is a string, and count is false, then we behave like .distinct().
	 *                  If keys is a positive sized array, we will return an array of arrays.
	 *                  If count is true, we will return an array of arrays where the last value is the count.
	 */
	DBQuery.prototype.distinct2 = function (keys, count) {
		var i = 0,
			j = 0,
			addDataKey,
			returnArray = [],
			tempArray = [],
			arrayOfValues = false,
			data = {
				keys : [],
				countKeys : [],
				counts : {}
			};

		// our data object contains keys that are numbers, so data[0] is an array containing values
		addDataKey = function (key, value) {
			var index = data[key].indexOf(value);
			if (index < 0) {
				data[key].push(value);
				index = data[key].length - 1;
			}
			return index;
		};

		// if passed a string, convert it into an array
		if (typeof keys === 'string') {
			keys = [keys];
		}

		// if keys is not a positive sized array by now, do nothing
		if (!isArray(keys) || keys.length === 0) {
			return returnArray;
		}

		// init our data object. it contains: keys, counts, and a list of indexes
		for (i = 0; i < keys.length; i++) {
			data[i] = [];
			data.keys.push(keys[i]);
		}

		// populate data object
		this.forEach(function (obj) {
			var i, found = false, countKey, values = [], indices = [];
			for (i = 0; i < data.keys.length; i++) {
				values[i] = getFromKeyString(obj, data.keys[i]);
				indices[i] = addDataKey(i, values[i]);
				if (!found && 'undefined' !== typeof values[i]) {
					found = true;
				}
			}
			// add item to our data object
			if (found) {
				countKey = indices.join(',');
				if (!data.counts.hasOwnProperty(countKey)) {
					data.counts[countKey] = 0;
					data.countKeys.push(countKey);
				}
				if (count) {
					data.counts[countKey]++;
				}
			}
		});

		// should we return an array of values?
		if (keys.length === 1 && !count) {
			arrayOfValues = true;
		}

		// convert data object into returnArray
		for (i = 0; i < data.countKeys.length; i++) {
			if (arrayOfValues) { // we return an array of values
				returnArray.push(data[0][data.countKeys[i]]);
			} else { // we return an array of arrays
				tempArray = data.countKeys[i].split(',');
				for (j = 0; j < tempArray.length; j++) {
					tempArray[j] = data[j][tempArray[j]];
				}
				if (count) {
					tempArray.push(data.counts[data.countKeys[i]]);
				}
				returnArray.push(tempArray);
			}
		}

		return returnArray;
	};

	/**
	 * @function
	 * @name distinct2
	 * @memberOf DBCollection
	 * @param keys {string|array} The array of dot delimited keys to get the distinct values for
	 * @param count {boolean} Whether or not to append
	 * @returns {array} If keys is a string, and count is false, then we behave like .distinct().
	 *                  If keys is a positive sized array, we will return an array of arrays.
	 *                  If count is true, we will return an array of arrays where the last value is the count.
	 */
	DBCollection.prototype.distinct2 = function (keys, count) {
		return this.find({}).distinct2(keys, count);
	};

}());