const BigQuery = require('BigQuery');
const getAllEventData = require('getAllEventData');
const logToConsole = require('logToConsole');

const eventData = getAllEventData();

const connectionInfo = {
  projectId: data.projectId, 
  datasetId: data.datasetId, 
  tableId: data.tableId
};

let row = {};

if (data.dataToInsert === 'allData') {
  row = eventData;
} else if (data.dataToInsert === 'customData') {
  if (data.customFields && data.customFields.length > 0) {
    data.customFields.forEach((field) => {
      row[field.eventParam] = field.paramValue;
    });
  } else {
    logToConsole('Warning: custom data selected but no fields defined.');
  }
}

const rows = [row];

const options = {
  ignoreUnknownValues: true,
  skipInvalidRows: false,
};

BigQuery.insert(connectionInfo, rows, options).then(() => {
  logToConsole('Success: Inserted into table : ', row);
  data.gtmOnSuccess();
}).catch((error) => {
  logToConsole('BigQuery Error: ', error);
  data.gtmOnFailure();
});
