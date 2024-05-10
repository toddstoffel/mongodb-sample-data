db.flights.aggregate([
  { $match: { year: 2020 } },
  {
    $group: {
      _id: "$carrier.airline",
      volume: { $sum: 1 },
      diverted: { $sum: "$diverted" },
      cancelled: { $sum: "$cancelled" },
    },
  },
  {
    $project: {
      _id: 0,
      airline: "$_id",
      flight_count: "$volume",
      cancelled_pct: {
        $round: [
          { $multiply: [{ $divide: ["$cancelled", "$volume"] }, 100] },
          2,
        ],
      },
      diverted_pct: {
        $round: [
          { $multiply: [{ $divide: ["$diverted", "$volume"] }, 100] },
          2,
        ],
      },
    },
  },
  { $sort: { flight_count: -1 } },
  {
    $group: {
      _id: null,
      total_volume: { $sum: "$flight_count" },
      flights: { $push: "$$ROOT" },
    },
  },
  { $unwind: "$flights" },
  {
    $project: {
      _id: 0,
      airline: "$flights.airline",
      flight_count: "$flights.flight_count",
      cancelled_pct: "$flights.cancelled_pct",
      diverted_pct: "$flights.diverted_pct",
      market_share_pct: {
        $round: [
          {
            $multiply: [
              { $divide: ["$flights.flight_count", "$total_volume"] },
              100,
            ],
          },
          2,
        ],
      },
    },
  },
  { $sort: { flight_count: -1 } },
]);