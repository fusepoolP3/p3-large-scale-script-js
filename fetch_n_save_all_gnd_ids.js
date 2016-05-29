/**************************************************
 * Retrieves all GND IDs in blocks of 10,000 and
 * saves each block to a file T0_results.n
 */

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

/*************************/
/** Declaring constants **/
/*************************/

// sparql endpoint host
var sparqlHost = 'ulcs.fusepool.info';
// sparql endpoint port
var sparqlPort = '8890';
// sparql endpoint path
var sparqlPath = '/sparql';
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
// relative path of folder containing the sparql files
var resourceFolder = 'sparql';
// relative path of folder containing the query results files
var resultsFolder = 'data';
// file names
var files = {
    T0: 'T0_select-all-GND-IDs-(milos).rq',
    T0_results: 'T0_results' 
};
// relative file paths
var filePaths = {
    T0: path.join(__dirname, resourceFolder, files.T0),
    T0_results: path.join(__dirname, resultsFolder, files.T0_results)
};
/********************/
/** Main variables **/
/********************/

// global array for storing the list of URIs
var URIs = new Array();
// local index for looping through the list of URIs
var index = 0;
// global index for looping through the list of URIs
var globalIndex = 0;
// Number of T0 select queries executed
var selectQueryCount = 0;
// limit for T0
var limit = 10000;
// global offset for T0
var offset = 0;
// sparql queries read from file
var queries = {
    T0: readQuery(filePaths.T0)
};

// STARTING SCRIPT
run();

/********************/
/** Main functions **/
/********************/

function run() {
    console.log('\n---== Starting Script ==---');
    queryURIs();
}

/**
 * Gets all the GND URIs using the T0 query.
 **/
function queryURIs() {
    selectQueryCount++;
    console.log('Executing T0 query (iteration: ' + selectQueryCount + ', offset: ' + offset + ') ...');
    fs.writeFile(filePaths.T0_results + '.' + selectQueryCount, '', function(err) {
	  if (err) throw err;
	});
    var query = replaceAll(queries.T0, '<offset>', offset);
    query = replaceAll(query, '<limit>', limit);
    // empty array
    URIs = new Array();
    // get POST data
    var data = getData(query, formats.JSON);
    var options = getOptions(data)
    // create HTTP request
    var request = http.request(options, function (response) {
	var data = '';
        response.setEncoding('utf8');
        response.on('data', function (chunk) {
	    data += chunk;
	  });
	response.on('end', function() {
            var json = JSON.parse(data);
            json = json.results.bindings;
            for (var i = 0; i < json.length; i++) {
                URIs.push(json[i].gndid.value);	
            }
            processQueryResults();
        });
    });

    request.on('error', function (e) {
        console.error('FATAL: Request returned with error while querying T0.');
        process.exit(1);
    });

    request.write(data);
    request.end();
}

function processQueryResults() {
    if (index < URIs.length) 
      console.log ('Writing query results to ' + files.T0_results + '.' + selectQueryCount);
    while (index < URIs.length) {
        // get current URI
        var currentUri = URIs[index];
        index++;
        globalIndex++;
	fs.appendFileSync(filePaths.T0_results + '.' + selectQueryCount, currentUri + '\n');
    }

    if (URIs.length > 0) {
      index = 0;
      // increase offset for T0
      offset += limit;
      queryURIs();
    }
    else {
      process.exit(0);
    }
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