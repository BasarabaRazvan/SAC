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
var client = new recombee.ApiClient(
	'idontknow-bazanoua', 
	'SyRHbZB7y0phdMJwBR0LTVat4WKCKO4LLJ4BaqkiJMGzXie1fE3pSzcv0ojyo39M', 
	{ region: 'us-west' }
);

const itemProperties = [
	{ name: 'title', type: 'string' },
	{ name: 'director', type: 'string' },
	{ name: 'date_added', type: 'string' },
	{ name: 'rating', type: 'string' },
	{ name: 'duration', type: 'string' },
	{ name: 'listed_in', type: 'string' },
	{ name: 'cast', type: 'string' },
	{ name: 'country', type: 'string' },
	{ name: 'release_year', type: 'int' },
	{ name: 'type', type: 'string' },
];

client.send(new rqs.Batch([
	new rqs.AddItemProperty('title', 'string'),
	new rqs.AddItemProperty('director', 'string'),
	new rqs.AddItemProperty('date_added', 'string'),
	new rqs.AddItemProperty('rating', 'string'),
	new rqs.AddItemProperty('duration', 'string'),
	new rqs.AddItemProperty('listed_in', 'string'),
	new rqs.AddItemProperty('cast', 'string'),
	new rqs.AddItemProperty('country', 'string'),
	new rqs.AddItemProperty('release_year', 'int'),
	new rqs.AddItemProperty('type', 'string'),
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
			'listed_in': item.listed_in || '',
			'cast': item.cast || '',
			'country': item.country || '',
			'release_year': item.release_year || 0,
			'type': item.type || '',
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
 
// User properties
const userProperties = [
	{ name: 'age', type: 'int' },
	{ name: 'country', type: 'string' },
	{ name: 'fav_director', type: 'string' },
	{ name: 'fav_genre', type: 'string' },
	{ name: 'fav_actor', type: 'string' },
	{ name: 'fav_type', type: 'string' },
	{ name: 'rating', type: 'string' },
	{ name: 'decade', type: 'string' },
];

// Add users
client.send(new rqs.Batch([
    new rqs.AddUser('user-1', userProperties),
    new rqs.AddUser('user-2', userProperties),
    new rqs.AddUser('user-3', userProperties),
    new rqs.AddUser('user-4', userProperties),
    new rqs.AddUser('user-5', userProperties),
    new rqs.AddUser('user-6', userProperties),
])).then(() => {
    console.log('Users added');
}).catch((err) => {
    console.error('Error adding users', err);
});
 
// Add user properties
client.send(new rqs.Batch([
    new rqs.AddUserProperty('age', 'int'),
    new rqs.AddUserProperty('country', 'string'),
    new rqs.AddUserProperty('fav_director', 'string'),
    new rqs.AddUserProperty('fav_genre', 'string'),
    new rqs.AddUserProperty('fav_actor', 'string'),
    new rqs.AddUserProperty('fav_type', 'string'),
    new rqs.AddUserProperty('rating', 'string'),
    new rqs.AddUserProperty('decade', 'string'),
])).then(() => {
    console.log('Users properties added');
}).catch((err) => {
    console.error('Error adding user properties', err);
});

const requestsUserProperty = [
	new rqs.SetUserValues('user-1', {
		'age': 10,
		'country': 'Japan',
		'fav_director': '-',
		'fav_genre': 'Anime Features',
		'fav_actor': '-',
		'fav_type': 'Movie',
		'rating': '-',
		'decade': '-',
	}, { cascadeCreate: true }),
	new rqs.SetUserValues('user-2', {
		'age': 20,
		'country': 'United States',
		'fav_director': '-',
		'fav_genre': 'Comedies',
		'fav_actor': 'Logan Browning',
		'fav_type': 'TV Show',
		'rating': 'TV-MA',
		'decade': '2010-2019',
	}, { cascadeCreate: true }),
	new rqs.SetUserValues('user-3', {
		'age': 30,
		'country': 'India',
		'fav_director': '-',
		'fav_genre': 'International Movies',
		'fav_actor': 'Leonardo DiCaprio',
		'fav_type': 'Movie',
		'rating': '-',
		'decade': '-',
	}, { cascadeCreate: true }),
	new rqs.SetUserValues('user-4', {
		'age': 40,
		'country': 'United States',
		'fav_director': 'Quentin Tarantino',
		'fav_genre': 'Action & Adventure',
		'fav_actor': 'Leonardo DiCaprio',
		'fav_type': 'Movie',
		'rating': 'TV-MA',
		'decade': '2020-2029',
	}, { cascadeCreate: true }),
	new rqs.SetUserValues('user-5', {
		'age': 50,
		'country': 'United States',
		'fav_director': '-',
		'fav_genre': 'Action & Adventure',
		'fav_actor': 'Leonardo DiCaprio',
		'fav_type': 'Movie',
		'rating': '-',
		'decade': '-',
	}, { cascadeCreate: true }),
];

client.send(new rqs.Batch(requestsUserProperty)).then(() => {
	console.log('Users properties uploaded');
}).catch((err) => {
	console.log('Error uploading properties', err);
});

const requestDetailsView = [
	new rqs.AddDetailView('user-1', 's52', {
		'duration': 99,
		'cascadeCreate': true,
	}),
	new rqs.AddDetailView('user-1', 's58', {
		'duration': 60,
		'cascadeCreate': true,
	}),
	new rqs.AddDetailView('user-2', 's10', {
		'duration': 98,
		'cascadeCreate': true,
	}),
	new rqs.AddDetailView('user-3', 's51', {
		'duration': 99,
		'cascadeCreate': true,
	}),
	new rqs.AddDetailView('user-4', 's58', {
		'duration': 60,
		'cascadeCreate': true,
	}),
	new rqs.AddDetailView('user-5', 's29', {
		'duration': 95,
		'cascadeCreate': true,
	}),
];

client.send(new rqs.Batch(requestDetailsView)).then(() => {
	console.log('Details view uploaded');
}).catch((err) => {
	console.log('Error uploading details view', err);
});


const requestAddRatings = [
	new rqs.AddRating('user-1', 's52', 5, {
		'cascadeCreate': true,
	}),
	new rqs.AddRating('user-1', 's58', 4, {
		'cascadeCreate': true,
	}),
	new rqs.AddRating('user-2', 's10', 5, {
		'cascadeCreate': true,
	}),
	new rqs.AddRating('user-3', 's51', 3, {
		'cascadeCreate': true,
	}),
	new rqs.AddRating('user-4', 's58', 4, {
		'cascadeCreate': true,
	}),
	new rqs.AddRating('user-5', 's29', 5, {
		'cascadeCreate': true,
	}),
];

client.send(new rqs.Batch(requestAddRatings)).then(() => {
	console.log('Ratings uploaded');
}).catch((err) => {
	console.log('Error uploading ratings', err);
});


client.send(new rqs.RecommendItemsToUser('user-1', 5, {
	cascadeCreate: true,
})).then((recommendedItems) => {
	console.log('Recommended items for user-1:\n', recommendedItems);
}).catch((err) => {
	console.log('Error getting recommendations', err);
});

client.send(new rqs.RecommendItemsToUser('user-2', 5, {
	cascadeCreate: true,
})).then((recommendedItems) => {
	console.log('Recommended items for user-2:\n', recommendedItems);
}).catch((err) => {
	console.log('Error getting recommendations', err);
});


client.send(new rqs.RecommendItemsToUser('user-3', 5, {
	cascadeCreate: true,
})).then((recommendedItems) => {
	console.log('Recommended items for user-3:\n', recommendedItems);
}).catch((err) => {
	console.log('Error getting recommendations', err);
});

client.send(new rqs.RecommendItemsToUser('user-4', 5, {
	cascadeCreate: true,
})).then((recommendedItems) => {
	console.log('Recommended items for user-4:\n', recommendedItems);
}).catch((err) => {
	console.log('Error getting recommendations', err);
});

client.send(new rqs.RecommendItemsToUser('user-5', 5, {
	cascadeCreate: true,
})).then((recommendedItems) => {
	console.log('Recommended items for user-5:\n', recommendedItems);
}).catch((err) => {
	console.log('Error getting recommendations', err);
});