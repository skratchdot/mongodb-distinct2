/*global DBCollection, DBQuery, print, tojson */
/*jslint indent: 4, plusplus: true, nomen: true */
/**
 * MongoDB - distinct2.js
 * 
 *      Version: 1.5
 *         Date: December 12, 2012
 *      Project: http://skratchdot.com/projects/mongodb-distinct2/
 *  Source Code: https://github.com/skratchdot/mongodb-distinct2/
 *       Issues: https://github.com/skratchdot/mongodb-distinct2/issues/
 * Dependencies: MongoDB v1.8+
 *               JSON2.js (https://github.com/douglascrockford/JSON-js)
 * 
 * Copyright (c) 2012 SKRATCHDOT.COM
 * Dual licensed under the MIT and GPL licenses:
 *   http://www.opensource.org/licenses/mit-license.php
 *   http://www.gnu.org/licenses/gpl.html
 */
(function () {
    'use strict';

        // config variables
    var currentDate,
        currentTick = (new Date()).getTime(),
        previousTick = (new Date()).getTime(),
        statusIntervalInMs = 10000,
        // functions
        isArray,
        getFromKeyString,
        getHashKey,
        printStatus,
        setStatusInterval;

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
                if (obj && typeof obj === 'object' && obj.hasOwnProperty(arr[i])) {
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
     * @name printStatus
     * @private
     * @param currentDocNum {number} - Number of the current document being processed
     * @param numDocs {number}       - Total number of documents being processed
     * @param distinctCount {number} - Total number of distinct items found so far
     */
    printStatus = function (currentDocNum, numDocs, distinctCount) {
        // Output some debugging info if needed
        if (statusIntervalInMs > 0) {
            currentDate = new Date();
            currentTick = currentDate.getTime();
            if (currentTick - previousTick > statusIntervalInMs) {
                print('Processed ' + currentDocNum + ' of ' + numDocs + ' document(s) and found ' + distinctCount + ' distinct items at ' + currentDate);
                previousTick = currentTick;
            }
        }
    };

    /**
     * @function
     * @name setStatusInterval
     * @private
     * @param intervalInMs {number} Will print out a status message after this many
     *                              milliseconds. If a non-positive number is passed in,
     *                              then no status messages will be printed.
     */
    setStatusInterval = function (intervalInMs) {
        if (typeof intervalInMs === 'number') {
            statusIntervalInMs = intervalInMs;
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
            currentDocNum = 0,
            numDocs = this.size(),
            distinctCount = 0,
            returnArray = [],
            tempArray = [],
            arrayOfValues = false,
            dataOrder = [],
            data = {};

        // if passed a string, convert it into an array
        if (typeof keys === 'string') {
            keys = [keys];
        }

        // if keys is not a positive sized array by now, do nothing
        if (!isArray(keys) || keys.length === 0) {
            return returnArray;
        }

        // update tick for printing status line
        previousTick = (new Date()).getTime();

        // populate data object
        this.forEach(function (obj) {
            var i, values = [], key = '', isDefined = false;
            for (i = 0; i < keys.length; i++) {
                values[i] = getFromKeyString(obj, keys[i]);
                if (typeof values[i] !== 'undefined') {
                    isDefined = true;
                }
            }
            if (isDefined) {
                key = getHashKey(values);
                if (data.hasOwnProperty(key)) {
                    data[key].count = data[key].count + 1;
                } else {
                    dataOrder.push(key);
                    data[key] = {
                        values : values,
                        count : 1
                    };
                }
            }
            // print status info
            printStatus(++currentDocNum, numDocs, dataOrder.length);
        });

        // should we return an array of values?
        if (keys.length === 1 && !count) {
            arrayOfValues = true;
        }

        for (i = 0; i < dataOrder.length; i++) {
            if (arrayOfValues) { // we return an array of values
                returnArray.push(data[dataOrder[i]].values[0]);
            } else { // we return an array of arrays
                tempArray = data[dataOrder[i]].values;
                if (count) {
                    tempArray.push(data[dataOrder[i]].count);
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
        var fields = {}, i, excludeId = true;
        if (typeof keys === 'string') {
            keys = [keys];
        }
        if (!isArray(keys)) {
            keys = [];
        }
        for (i = 0; i < keys.length; i++) {
            fields[keys[i]] = 1;
            if (keys[i] === '_id') {
                excludeId = false;
            }
        }
        if (!excludeId) {
            fields._id = 0;
        }
        return this.find({}, fields).distinct2(keys, count);
    };

    // Attach setStatusInterval to both versions of distinct2
    DBQuery.prototype.distinct2.setStatusInterval = setStatusInterval;
    DBCollection.prototype.distinct2.setStatusInterval = setStatusInterval;

    // set the correct getHashKey function
    if (typeof JSON !== 'undefined' && typeof JSON.stringify === 'function') {
        getHashKey = JSON.stringify;
    } else if (typeof tojson === 'function') {
        getHashKey = tojson;
    } else {
        getHashKey = function (obj) {
            return obj;
        };
    }
}());