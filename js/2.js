db.flights.aggregate([
  {
    $match: {
      has_delay: true,
    },
  },
  {
    $group: {
      _id: {
        airline: "$carrier.airline",
        year: "$year",
        delay_type: {
          $switch: {
            branches: [
              { case: { $gt: ["$carrier_delay", 0] }, then: "Airline Delay" },
              {
                case: { $gt: ["$late_aircraft_delay", 0] },
                then: "Late Aircraft Delay",
              },
              { case: { $gt: ["$nas_delay", 0] }, then: "Air System Delay" },
              { case: { $gt: ["$weather_delay", 0] }, then: "Weather Delay" },
            ],
            default: "Other Delay",
          },
        },
      },
      delay: { $sum: 1 },
    },
  },
  {
    $sort: {
      "_id.airline": 1,
      "_id.year": 1,
      "_id.delay_type": 1,
    },
  },
  {
    $project: {
      _id: 0,
      airline: "$_id.airline",
      year: "$_id.year",
      delay_type: "$_id.delay_type",
      delay: "$delay",
    },
  },
]);