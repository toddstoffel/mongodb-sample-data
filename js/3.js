db.flights.aggregate([
  {
    $match: {
      $and: [
        { arr_delay: { $gt: 0 } },
        { "dest.code": { $in: ["SFO", "OAK", "SJC"] } },
        { year: 2020 },
      ],
    },
  },
  {
    $addFields: {
      monthname: {
        $let: {
          vars: {
            monthsInString: [
              null,
              "January",
              "February",
              "March",
              "April",
              "May",
              "June",
              "July",
              "August",
              "September",
              "October",
              "November",
              "December",
            ],
          },
          in: {
            $arrayElemAt: ["$$monthsInString", "$month"],
          },
        },
      },
    },
  },
  {
    $project: {
      _id: 0,
      dest: "$dest.code",
      month: 1,
      monthname: 1,
      scheduled_arrival_hr: { $toInt: { $substr: ["$crs_arr_time", 0, 2] } },
      arr_delay: 1,
    },
  },
  {
    $group: {
      _id: {
        dest: "$dest",
        month: "$month",
        monthname: "$monthname",
        scheduled_arrival_hr: "$scheduled_arrival_hr",
      },
      avg_arr_delay: {
        $avg: "$arr_delay",
      },
      max_arr_delay: { $max: "$arr_delay" },
    },
  },
  {
    $project: {
      _id: 0,
      dest: "$_id.dest",
      month: "$_id.month",
      monthname: "$_id.monthname",
      scheduled_arrival_hr: "$_id.scheduled_arrival_hr",
      avg_arr_delay: {
        $round: ["$avg_arr_delay", 6],
      },
      max_arr_delay: 1,
    },
  },
  {
    $sort: {
      dest: 1,
      month: 1,
      scheduled_arrival_hr: 1,
    },
  },
]);