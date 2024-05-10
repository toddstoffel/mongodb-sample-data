db.flights.aggregate([
  {
    $match: {
      $and: [
        { arr_delay: { $gt: 0 } },
        { "dest.code": { $in: ["SFO", "OAK", "SJC"] } },
        { year: 2020 },
        { month: 11 },
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
      monthname: 1,
      day: 1,
      arr_delay: 1,
    },
  },
  {
    $group: {
      _id: {
        dest: "$dest",
        monthname: "$monthname",
        day: "$day",
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
      monthname: "$_id.monthname",
      day: "$_id.day",
      avg_arr_delay: { $round: ["$avg_arr_delay", 6] },
      max_arr_delay: 1,
    },
  },
  {
    $sort: {
      dest: 1,
      monthname: 1,
      day: 1,
    },
  },
]);