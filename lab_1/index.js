const express = require('express');
const recombee = require('recombee-api-client');
const fs = require('fs');
const csv = require('fast-csv');

const app = express();
const port = 8080;

app.listen(port, () => {
    console.log(`Now listening on port ${port}`);
});

const rqs = recombee.requests;
const client = new recombee.ApiClient(
	'w-movies',
	'r8SmLq7qORaqMIzNOcSgWvy00ph5EB2n3Ioat9X8wsIH304bppRoClw424cqSWFn',
	{ region: 'eu-west' }
);

const itemProperties = [
	{ name: 'title', type: 'string' },
	{ name: 'director', type: 'string' },
	{ name: 'date_added', type: 'string' },
	{ name: 'rating', type: 'string' },
	{ name: 'duration', type: 'string' },
];

client.send(new rqs.Batch([
	new rqs.AddItemProperty('title', 'string'),
	new rqs.AddItemProperty('director', 'string'),
	new rqs.AddItemProperty('date_added', 'string'),
	new rqs.AddItemProperty('rating', 'string'),
	new rqs.AddItemProperty('duration', 'string'),
]));

const csvStream = csv.parseFile('./netflix_titles.csv', { headers: true, maxRows: 100 })
const parseCsv = async () => {
	return new Promise((resolve, reject) => {
		const items = [];
		csvStream.on('data', (data) => {
			items.push(data);
		});
		csvStream.on('end', () => {
			resolve(items);
		});
		csvStream.on('error', (err) => {
			reject(err);
		});
	});
};

let items = [];

const uploadItems = async () => {
	items = await parseCsv();
	const requests = items.map((item) => {
			return new rqs.AddItem(item.show_id, itemProperties);
	});
	return client.send(new rqs.Batch(requests));
}

const uploadProperties = (items) => {
	const requests = items.map((item) => {
		const itemValues = {
			'title': item.title || '',
			'director': item.director || '',
			'date_added': item.date_added || '',
			'rating': item.rating || '',
			'duration': item.duration || '',
		};
		return new rqs.SetItemValues(item.show_id, itemValues, { cascadeCreate: true });
	});
	return client.send(new rqs.Batch(requests));
}

uploadItems().then(() => {
	console.log('Items uploaded');
	uploadProperties(items).then(() => {
		console.log('Properties uploaded');
	}).catch((err) => {
		console.log('Error uploading properties', err);
	});
}).catch((err) => {
	console.log('Error uploading items', err);
});
