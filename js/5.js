db.flights.aggregate([
  {
    $match: {
      "dest.state": "CA",
      year: 2020,
    },
  },
  {
    $group: {
      _id: {
        airline: "$carrier.airline",
        airport: "$dest.airport",
      },
      volume: { $sum: 1 },
      total_arrival_delay: { $sum: "$arr_delay" },
    },
  },
  {
    $project: {
      _id: 1,
      volume: 1,
      avg_arrival_delay: { $divide: ["$total_arrival_delay", "$volume"] },
    },
  },
  {
    $project: {
      _id: 1,
      volume: 1,
      avg_arrival_delay: { $round: ["$avg_arrival_delay", 6] },
    },
  },
  {
    $sort: {
      "_id.airline": 1,
      "_id.airport": 1,
    },
  },
]);