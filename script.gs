//Staging folder in drive

function mainScript () {

//To fetch the folder ID and .json files

var folder= DriveApp.getFolderById("1mnQxGNO_xl9famXXm07xwsme5Q9UAFZY");
var files = folder.getFiles();

while(files.hasNext()){
    var file=files.next();
    makeBigQueryFileFromDrive(file);
    var folderDestination= DriveApp.getFolderById("1ThNRmt8V-UZlhzRwEYxv4cFLDrJwFn3w");
    file.makeCopy(new Date()+" data.json", folderDestination)
    file.setTrashed(true);
  }
}

//To prepare the file from drive

function makeBigQueryFileFromDrive(file) {


      
  // get the data & build the json object
  var dataObject = JSON.parse(file.getBlob().getDataAsString());
  var data = [];
  
  data.push(dataObject);
  
  
  Logger.log(data);
  

  // this will create this table if needed
  var tableReference = {
    projectId: "valeo-smartbi-sbi0000",
    datasetId: "DEV",
    tableId: "JSON_DATA"
};

Logger.log(tableReference)


  // load it to bigquery
  var jobs = loadToBigQuery (tableReference, data);
  Logger.log(jobs);
  // keep an eye on it
  Logger.log('status of job can be found here: https://bigquery.cloud.google.com/jobs/' + tableReference.projectId);
}



function loadToBigQuery (tableReference,data) {

  // These are the data types I'll support for now.
  var bqTypes = {
    number:"FLOAT",
    string:"STRING",
    time:"TIMESTAMP"
    
  };
  
// figure out the schema from the JSON data- assuming that the data is all consistent types
  var model = data[0];
 Logger.log(model)
  var fields = Object.keys(model).reduce(function(p,c) {
    var t = typeof model[c];
    if (!bqTypes[t]) {
      throw 'unsupported type ' + t;
    }
    p.push ( {name:c, type: bqTypes[t]} );
    return p;
  },[]);

 // the load job
  var job = {
    configuration: {
      load: {
        destinationTable: tableReference,
        sourceFormat: "NEWLINE_DELIMITED_JSON",
        writeDisposition: "WRITE_APPEND",
        schema:{
          fields:fields
        }
      },
    }
  };
  
  // there is a max size that a urlfetch post size can be
  var MAX_POST = 1024 * 1024 * 8;
  
  // now we can make delimted json, but chunk it up
  var chunk = '', jobs = [];
  data.forEach (function (d,i,a) {
    
    // convert to string with "\n"
    var chunklet = JSON.stringify(d) + "\n";
    if (chunklet.length  > MAX_POST) {
      throw 'chunklet size ' + chunklet.length + ' is greater than max permitted size ' + MAX_POST;
    }
    
    // time to flush?
    if (chunklet.length + chunk.length > MAX_POST) {
      jobs.push ( BigQuery.Jobs.insert(job, tableReference.projectId, Utilities.newBlob(chunk)) );
      chunk = "";
      // after the first , we move to append
      job.configuration.load.writeDisposition = "WRITE_APPEND";
    }
    
    // add to the pile
    chunk += chunklet;
   
  });
  
  // finish off
  if (chunk.length) {
    jobs.push ( BigQuery.Jobs.insert(job, tableReference.projectId, Utilities.newBlob(chunk)) );
  }
  return jobs;
  
  } 
