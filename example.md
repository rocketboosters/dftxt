account_id    created_at                plan_id         country
&dtype=str    &dtype=timestamp                          &dtype=str
1             2023-04-23T13:01:59Z      foo bar         United States
1             2023-04-23T13:01:59Z      foo bar         United States
2             2023-10-01T01:21:00Z      spam ham        England
3             2023-11-10T00:00:00Z      lorem ipsum     South Korea
4             2023-12-09T06:42:02Z      free            Mexico

---

account_id    converted_on    Monthly Revenue         active
&repeat      &dtype=date     &dtype=decimal(12, 3)   &dtype=boolean
1             2023-04-23      25.490                  True
1             2023-04-23      25.490                  True
2             2023-10-00      99.990                  False
3             2023-11-10      149.990                 True
4             None            None                    None
