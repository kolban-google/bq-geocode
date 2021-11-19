const { BigQuery } = require('@google-cloud/bigquery');
const  MapsClient = require("@googlemaps/google-maps-services-js").Client;
const util = require("util");

const mapsClient = new MapsClient();

async function geocode(address) {
    const res = await mapsClient.geocode({
        params: {
            "key": "AIza***",
            "address": address
        }
    });
    
    return res.data.results[0].geometry.location;
} // Geocode

async function loadFromBQ() {
    const bigquery = new BigQuery();
    const query = `SELECT Street,City,State,Postal,Country
        FROM \`test1-305123.maps.addresses\`
        LIMIT 10`;

    const options = {
        query: query,
        location: 'US',
    };

    const [job] = await bigquery.createQueryJob(options);

    const [rows] = await job.getQueryResults();

    rows.forEach(async row => {
        const address = `${row.Street} ${row.City} ${row.State} ${row.Postal==null?"":row.Postal} ${row.Country==null?"":row.Country}`
        const location = await geocode(address);
        console.log(`${row.Street},${row.City},${row.State},${row.Postal==null?"":row.Postal},${row.Country==null?"":row.Country},${location.lat},${location.lng}`)
    });
} // loadFromBQ

loadFromBQ();