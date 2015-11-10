/**************************/
/** Loading core modules **/
/**************************/

// module for file I/O operations
var fs = require('fs');
// module for handling and transforming file paths
var path = require('path');
// module for handling HTTP connections
var http = require('http');
// module for dealing with query strings
var querystring = require('querystring');
// module for reading console input
var readline = require('readline');

/*************************/
/** Declaring constants **/
/*************************/

// sparql endpoint host
var sparqlHost = 'ulcs.fusepool.info';
// sparql endpoint port
var sparqlPort = '8890';
// sparql endpoint path
var sparqlPath = '/sparql';
// sparql username for authentication
var sparqlUser = '';
// sparql password for authentication
var sparqlPass = '';
// basic auth header for sparql
var basicAuth = '';
// timeout parameter of sparql requests (15 mins)
var timeout = 15 * 60 * 1000;
// format parameter of sparql requests
var formats = {
    JSON: 'application/json',
    TURTLE: 'text/turtle',
    RDF: 'application/rdf+xml',
    FORM: 'application/x-www-form-urlencoded',
    AUTO: 'auto'
};
// parameter that needs to be replaced in the queries
var staticUri = 'http://d-nb.info/gnd/118529692';
// relative path of folder containing the sparql files
var resourceFolder = 'sparql';
// file names
var files = {
    T0: 'T0_select-all-GND-IDs-(milos).rq',
    T02: 'T0-2_clear-RDF-Graph-for-a-GND-ID-(milos).rq',
    T1: 'T1_create-RDF-Graph-for-a-GND-ID-(milos).rq',
    T3: 'T3_data-from-DBpedia-(milos)-insert.rq',
    T32: 'T3-2_textsDBpedia-incl-subjects+categories-(carl+milos)-insert.rq',
    T4: 'T4_coocConcepts-(milos)-insert.rq',
    T43: 'T4-3_coocConcepts_w-distance-(milos)-insert.rq',
    T44DDC: 'T4-4_coocConcepts_w-distance-add-type-DDC-(milos)-insert.rq',
    T44GND: 'T4-4_coocConcepts_w-distance-add-type-GND-(milos)-insert.rq',
    T44RVK: 'T4-4_coocConcepts_w-distance-add-type-RVK-(milos)-insert.rq',
    T44SSG: 'T4-4_coocConcepts_w-distance-add-type-SSG-(milos)-insert.rq',
    T5: 'T5_getTitlesByConcept-(milos)-insert.rq'
};
// relative file paths
var filePaths = {
    T0: path.join(__dirname, resourceFolder, files.T0),
    T02: path.join(__dirname, resourceFolder, files.T02),
    T1: path.join(__dirname, resourceFolder, files.T1),
    T3: path.join(__dirname, resourceFolder, files.T3),
    T32: path.join(__dirname, resourceFolder, files.T32),
    T4: path.join(__dirname, resourceFolder, files.T4),
    T43: path.join(__dirname, resourceFolder, files.T43),
    T44DDC: path.join(__dirname, resourceFolder, files.T44DDC),
    T44GND: path.join(__dirname, resourceFolder, files.T44GND),
    T44RVK: path.join(__dirname, resourceFolder, files.T44RVK),
    T44SSG: path.join(__dirname, resourceFolder, files.T44SSG),
    T5: path.join(__dirname, resourceFolder, files.T5)
};

/********************/
/** Main variables **/
/********************/

// if false the script will read the URIs from the data
// folder using the resultT0, otherwise runs T0 (from console input)
var RUN_T0;
// stored result of T0 query (data folder)
var resultT0 = 'T0_run.txt';
// global array for storing the list of URIs
var URIs = new Array();
// local index for looping through the list of URIs
var index = 0;
// global index for looping through the list of URIs
var globalIndex = 0;
// global variable to indicate first cycle after T0
var first = true;
// limit for T0
var limit = 10;
// global offset for T0
var offset = 0;
// flags for synchronization
var readyFlags = {
    T0: false,
    T02: false,
    T1: false,
    T3: false,
    T32: false,
    T4: false,
    T43: false,
    T44DDC: false,
    T44GND: false,
    T44RVK: false,
    T44SSG: false,
    T5: false
};
// sparql queries read from file
var queries = {
    T0: readQuery(filePaths.T0),
    T02: readQuery(filePaths.T02),
    T1: readQuery(filePaths.T1),
    T3: readQuery(filePaths.T3),
    T32: readQuery(filePaths.T32),
    T4: readQuery(filePaths.T4),
    T43: readQuery(filePaths.T43),
    T44DDC: readQuery(filePaths.T44DDC),
    T44GND: readQuery(filePaths.T44GND),
    T44RVK: readQuery(filePaths.T44RVK),
    T44SSG: readQuery(filePaths.T44SSG),
    T5: readQuery(filePaths.T5)
};

// STARTING SCRIPT
initialize();

/********************/
/** Main functions **/
/********************/

/**
 * Get user inputs before starting execution.
 */
function initialize() {
    console.log('\n---== Starting Script ==---');
    var rl = readline.createInterface({input: process.stdin, output: process.stdout});

    console.log('\nPlease provide credential for "' + sparqlHost + ':' + sparqlPort + sparqlPath + '" for basic HTTP authentication!');
    // get credentials from console input
    rl.question('Username: ', function (answer1) {
        sparqlUser = answer1;
        rl.question('Password: ', function (answer2) {
            sparqlPass = answer2;
            // run T0 from query or from file
            console.log('\nDo you want to execute T0 query? (If not the script will use the results in "' + resultT0 + '" file!)');
            rl.question('Run T0? (y/n): ', function (answer3) {
                RUN_T0 = (answer3 == 'y' || answer3 == 'Y') ? true : false;
                rl.close();
                start();
            });
        });
    });
}

/**
 * Starts the script by getting the list of URIs either
 * querying it from the SPARQL endpoint using T0 or reading
 * it from file. (The file contains the result of T0 as plain text.) 
 **/
function start() {
    console.time('Execution time');
    basicAuth = 'Basic ' + new Buffer(sparqlUser + ':' + sparqlPass).toString('base64');
    if (RUN_T0) {
        console.log('\nExecuting T0 query...');
        queryURIs();
    }
    else {
        console.log('\nReading "' + resultT0 + '" file...');
        readURIs();
    }
}

/**
 * Gets all the URIs using the T0 query.
 **/
function queryURIs() {
    var query = replaceAll(queries.T0, '<offset>', offset);
    query = replaceAll(query, '<limit>', limit);
    // empty array
    URIs = new Array();
    // get POST data
    var data = getData(query, formats.JSON);
    var options = getOptions(data)
    // create HTTP request
    var request = http.request(options, function (response) {
        response.setEncoding('utf8');
        response.on('data', function (data) {
            var json = JSON.parse(data);
            json = json.results.bindings;
            for (var i = 0; i < json.length; i++) {
                URIs.push(json[i].gndid.value);
            }
            // indicate first cycle after T0
            first = true;
            syncCallback();
        });
    });

    request.on('error', function (e) {
        console.error('FATAL: Request returned with error while querying T0.');
        process.exit(1);
    });

    request.write(data);
    request.end();
}

/**
 * Reads all the URIs from file. (Result of T0.)
 **/
function readURIs() {
    var filePath = path.join(__dirname, '/data', resultT0);
    // read file asynchronously
    fs.readFile(filePath, 'utf-8', function (error, data) {
        // if file cannot be read abort with fatal error
        if (error) {
            console.error('FATAL: Could not read file "' + filePath + '"');
            process.exit(1);
        }
        // if data is empty abort with fatal error
        if (isEmpty(data)) {
            console.error('FATAL: File is empty "' + filePath + '"');
            process.exit(1);
        }
        // set global list of URIs
        URIs = data.split('\r\n');
        syncCallback();
    });
}

/**
 * Replaces the static URI in the query and starts the first
 * it asynchronously. (Clear query.)
 **/
function startAsyncQueriesPart0(currentUri) {
    console.log((globalIndex + 1) + '. <' + currentUri + '>');
    // T0-2 query
    var query = replaceAll(queries.T02, staticUri, currentUri);
    runQuery(query, 'T02');
}

/**
 * Replaces the static URI in the queries and starts the first
 * group asynchronously.
 **/
function startAsyncQueriesPart1(currentUri) {
    // T1 query
    var query = replaceAll(queries.T1, staticUri, currentUri);
    runQuery(query, 'T1');
    // T3 query
    query = replaceAll(queries.T3, staticUri, currentUri);
    runQuery(query, 'T3');
    // T32 query
    query = replaceAll(queries.T32, staticUri, currentUri);
    query = replaceURISegments(query, staticUri, currentUri);
    runQuery(query, 'T32');
    // T4 query
    query = replaceAll(queries.T4, staticUri, currentUri);
    runQuery(query, 'T4');
    // T43 query
    query = replaceAll(queries.T43, staticUri, currentUri);
    runQuery(query, 'T43');
    // T5 query
    query = replaceAll(queries.T5, staticUri, currentUri);
    runQuery(query, 'T5');
}

/**
 * Replaces the static URI in the queries and starts the second
 * group asynchronously.
 **/
function startAsyncQueriesPart2(currentUri) {
    // T44DDC query
    var query = replaceAll(queries.T44DDC, staticUri, currentUri);
    runQuery(query, 'T44DDC');
    // T44GND query
    query = replaceAll(queries.T44GND, staticUri, currentUri);
    runQuery(query, 'T44GND');
    // T44RVK query
    query = replaceAll(queries.T44RVK, staticUri, currentUri);
    runQuery(query, 'T44RVK');
    // T44SSG query	
    query = replaceAll(queries.T44SSG, staticUri, currentUri);
    runQuery(query, 'T44SSG');
}

/**
 * Runs the supplied query asynchronously.
 **/
function runQuery(query, queryName) {
    var data = getData(query, formats.AUTO);
    var options = getOptions(data)
    // create HTTP request
    var request = http.request(options, function (response) {
        if (response.statusCode == 200) {
            console.log('\t' + queryName + ' done');
        }
        else {
            console.error('\tERROR: Response returned with status code ' + response.statusCode + ' for query "' + queryName + '"');
        }
        // set query ready flag
        readyFlags[queryName] = true;
        // synchronize with other threads
        syncCallback();
    });

    request.on('error', function (e) {
        console.error('\tERROR: Request returned with error for query "' + queryName + '"');
        readyFlags[queryName] = true;
        syncCallback();
    });

    request.write(data);
    request.end();
}

/**
 * Synchronizes the asynchronously started queries. It also keeps
 * the first and second group of queries from running concurrently.
 **/
function syncCallback() {
    // if the first query have finished call the first group with the same URI (clear query)
    if (readyFlags['T02']) {
        // reset ready flags
        readyFlags['T02'] = false;
        // get current URI
        var currentUri = URIs[index];
        // start the second group of queries
        startAsyncQueriesPart1(currentUri);
        return;
    }
    // if all queries have finished in the first group call the second group with the same URI
    if (readyFlags['T1'] && readyFlags['T3'] && readyFlags['T32'] && readyFlags['T4'] && readyFlags['T43'] && readyFlags['T5']) {
        // reset ready flags
        readyFlags['T1'] = false;
        readyFlags['T3'] = false;
        readyFlags['T32'] = false;
        readyFlags['T4'] = false;
        readyFlags['T43'] = false;
        readyFlags['T5'] = false;
        // get current URI
        var currentUri = URIs[index];
        // start the second group of queries
        startAsyncQueriesPart2(currentUri);
        return;
    }
    // if all queries have finished in the second group call the first group with the next URI
    if (readyFlags['T44DDC'] && readyFlags['T44GND'] && readyFlags['T44RVK'] && readyFlags['T44SSG']) {
        // reset ready flags
        readyFlags['T44DDC'] = false;
        readyFlags['T44GND'] = false;
        readyFlags['T44RVK'] = false;
        readyFlags['T44SSG'] = false;
        // increase index
        index++;
        // increase global index
        globalIndex++;
        // go to the next URI if there is any
        if (index < URIs.length) {
            // get current URI
            var currentUri = URIs[index];
            // start the first group of queries
            startAsyncQueriesPart0(currentUri);
            return;
        }
        else {
            if (RUN_T0) {
                if (URIs.length > 0) {
                    index = 0;
                    // increase offset for T0
                    offset += limit;
                    console.log('Executing T0 query...');
                    queryURIs();
                }
                else {
                    console.timeEnd('Execution time');
                    process.exit(0);
                }
            }
            else {
                console.timeEnd('Execution time');
                process.exit(0);
            }
        }
    }
    // if this is the first run (after T0)
    if (first) {
        first = false;
        console.log('Iterate through each GND-ID...');
        // get current URI
        var currentUri = URIs[index];
        // start the first group of queries
        startAsyncQueriesPart0(currentUri);
        return;
    }
}

/**
 * Replaces the last segment of uri1 with the last
 * segment of uri2 in the query text. (For query T32.)
 **/
function replaceURISegments(query, uri1, uri2) {
    // get last segment from uri1
    var str1 = uri1.substr(uri1.lastIndexOf("/") + 1);
    // get last segment from uri2
    var str2 = uri2.substr(uri2.lastIndexOf("/") + 1);
    // replace all
    return replaceAll(query, str1, str2);
}

/*********************/
/** Other functions **/
/*********************/

/**
 * Create URI encoded data for HTTP POST.
 **/
function getData(query, format) {
    return data = querystring.stringify({
        query: query,
        format: format,
        timeout: timeout
    });
}

/**
 * Create options for HTTP POST.
 **/
function getOptions(data) {
    return options = {
        host: sparqlHost,
        port: sparqlPort,
        path: sparqlPath,
        method: 'POST',
        headers: {
            'Authorization': basicAuth,
            'Content-Type': formats.FORM,
            'Content-Length': Buffer.byteLength(data)
        }
    };
}

/**
 * Reads file synchronously from supplied path.
 **/
function readQuery(filePath) {
    var data = fs.readFileSync(filePath, 'utf-8');
    if (isEmpty(data)) {
        console.error('FATAL: File is empty "' + filePath + '"');
        process.exit(1);
    }
    // remove comment lines
    data = data.replace(/^#.*$/gm, '');
    // remove blank lines	
    data = data.replace(/^\s*$/gm, '');
    return data;
}

/**
 * Replaces all occurrences of str1 to str2 in text.
 **/
function replaceAll(text, str1, str2) {
    var regex = new RegExp(str1, 'g');
    return text.replace(regex, str2);
}

/**
 * Checks if variable is empty.
 **/
function isEmpty(data) {
    if (typeof data === 'undefined' || data === '' || data === null || data.length == 0) {
        return true;
    }
    return false;
}