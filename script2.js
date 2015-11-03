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
// TLDPC host
var tldpcHost = 'ulcs.fusepool.info';
// TLDPC port
var tldpcPort = '8181';
// TLDPC path
var tldpcPath = '/DAV/home/fusepool/ldp/FU_Berlin_Extraction_Pipeline_Container';
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
    T6: 'T6_construct-query.rq'
};
// relative file paths
var filePaths = {
    T0: path.join(__dirname, resourceFolder, files.T0),
    T6: path.join(__dirname, resourceFolder, files.T6)
};
/********************/
/** Main variables **/
/********************/

// if false the script will read the URIs from the data
// folder using the resultT0, otherwise runs T0
var RUN_T0 = false;
// stored result of T0 query (data folder)
var resultT0 = 'T0_test.txt';
// global array for storing the list of URIs
var URIs = new Array();
// global index for looping through the list of URIs
var index = 0;
// global variable to indicate first cycle after T0
var first = true;
// sparql queries read from file
var queries = {
    T0: readQuery(filePaths.T0),
    T6: readQuery(filePaths.T6)
};

// STARTING SCRIPT
start();

/********************/
/** Main functions **/
/********************/

/**
 * Starts the script by getting the list of URIs either
 * querying it from the SPARQL endpoint using T0 or reading
 * it from file. (The file contains the result of T0 as plain text.) 
 **/
function start() {
    console.time('DONE');
    console.log('Initializing...');
    console.log('START');
    if (RUN_T0) {
        queryURIs();
    }
    else {
        readURIs();
    }
}

/**
 * Gets all the URIs using the T0 query.
 **/
function queryURIs() {
    var data = getData(queries.T0, formats.JSON);
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
 * Runs the select query to retrieve results from the sparql endpoint.
 **/
function getGNDResult(query, id) {
    console.log((index + 1) + '. <' + id + '>');
    var data = getData(query, formats.TURTLE);
    var options = getOptions(data)
    // create HTTP request
    var request = http.request(options, function (response) {
        if (response.statusCode == 200) {
            var data = '';
            response.setEncoding('utf8');
            response.on('data', function (chunk) {
                data += chunk;
            });
            response.on('end', function () {
                console.log('\t' + id + ' select done');
                postGNDResult(data, id);
            });
        }
        else {
            console.error('\tERROR: Response returned with status code ' + response.statusCode + ' for ' + id);
            syncCallback();
        }
    });

    request.on('error', function (e) {
        console.error('\tERROR: Request returned with error for ' + id);
        syncCallback();
    });

    request.write(data);
    request.end();
}

/**
 * Posts the selected results to the TLDPC.
 **/
function postGNDResult(rdf, id) {
    var options = {
        host: tldpcHost,
        port: tldpcPort,
        path: tldpcPath,
        method: 'POST',
        headers: {
            'Slug': id + '.ttl',
            //'Link': '<http://www.w3.org/ns/ldp#BasicContainer>; rel=\'type\'',
            'Content-Type': formats.TURTLE,
            'Content-Length': Buffer.byteLength(rdf)
        }
    };
    // create HTTP request
    var request = http.request(options, function (response) {
        if (response.statusCode == 201) {
            console.log('\t' + id + ' insert done');
        }
        else {
            console.error('\tERROR: Response returned with status code ' + response.statusCode + ' for ' + id);
        }
        // synchronize with other threads
        syncCallback();
    });

    request.on('error', function (e) {
        console.error('\tERROR: Request returned with error for ' + id);
        syncCallback();
    });

    request.write(rdf);
    request.end();
}

/**
 * Synchronizes the asynchronously started queries. It also keeps
 * the first and second group of queries from running concurrently.
 **/
function syncCallback() {
    // go to the next URI if there is any
    if (index < URIs.length) {
        // get current URI
        var currentUri = URIs[index];
        var id = currentUri.substr(currentUri.lastIndexOf('/') + 1);
        var query = replaceAll(queries.T6, staticUri, currentUri);
        getGNDResult(query, id);
        index++;
        return;
    }
    else {
        console.timeEnd('DONE');
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