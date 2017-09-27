# Split Column filter plugin for Embulk

A filter plugin for Embulk to split one string column to several any type columns.

## Configuration

- **delimiter**: delimiter for split column (string, required, default: ',')
- **is_skip**: if true, skip the line when output_columns num and split target column num are not matched. if false, throw the exception (boolean, optional, default: true)
- **target_key**: string column key you want to split(string, required)
- **output_columns**: description (array of hash, required)
  - This values is same for columns in parser

## Example

Say input.csv is as follows:

```
id,account,time,purchase,comment
1,32864,2015-01-27 19:23:49,20150127,a|1|1.1|True|2016-01-26
2,14824,2015-01-27 19:01:23,20150127,b|2|2.2|False|2016-01-27
3,27559,2015-01-28 02:20:02,20150128,c|3|3.3|False|2016-01-28
4,11270,2015-01-29 11:54:36,20150129,d|4|4.4|True|2016-01-29
```

In parse phase, split by ',':

```yaml
parser:
  type: csv
  delimiter: ','
  columns:
  - {name: id, type: long}
  - {name: account, type: long}
  - {name: time, type: timestamp, format: '%Y-%m-%d %H:%M:%S'}
  - {name: purchase, type: timestamp, format: '%Y%m%d'}
  - {name: comment, type: string}
```
```
+---------+--------------+-------------------------+-------------------------+--------------------------+
| id:long | account:long |          time:timestamp |      purchase:timestamp |           comment:string |
+---------+--------------+-------------------------+-------------------------+--------------------------+
|       1 |       32,864 | 2015-01-27 19:23:49 UTC | 2015-01-27 00:00:00 UTC |  a|1|1.1|True|2016-01-26 |
|       2 |       14,824 | 2015-01-27 19:01:23 UTC | 2015-01-27 00:00:00 UTC | b|2|2.2|False|2016-01-27 |
|       3 |       27,559 | 2015-01-28 02:20:02 UTC | 2015-01-28 00:00:00 UTC | c|3|3.3|False|2016-01-28 |
|       4 |       11,270 | 2015-01-29 11:54:36 UTC | 2015-01-29 00:00:00 UTC |  d|4|4.4|True|2016-01-29 |
+---------+--------------+-------------------------+-------------------------+--------------------------+
```

In additionally, you want to split comment by '|' using split_column filter:

```yaml
filters:
  - type: split_column
    delimiter: '|'
    is_skip: true
    target_key: comment
    output_columns:
      - {name: alph, type: string}
      - {name: num, type: long}
      - {name: dbl, type: double}
      - {name: bool, type: boolean}
      - {name: ts, type: timestamp, format: '%Y-%m-%d'}
```
```
+---------+--------------+-------------------------+-------------------------+-------------+----------+------------+--------------+-------------------------+
| id:long | account:long |          time:timestamp |      purchase:timestamp | alph:string | num:long | dbl:double | bool:boolean |            ts:timestamp |
+---------+--------------+-------------------------+-------------------------+-------------+----------+------------+--------------+-------------------------+
|       1 |       32,864 | 2015-01-27 19:23:49 UTC | 2015-01-27 00:00:00 UTC |           a |        1 |        1.1 |         true | 2016-01-26 00:00:00 UTC |
|       2 |       14,824 | 2015-01-27 19:01:23 UTC | 2015-01-27 00:00:00 UTC |           b |        2 |        2.2 |        false | 2016-01-27 00:00:00 UTC |
|       3 |       27,559 | 2015-01-28 02:20:02 UTC | 2015-01-28 00:00:00 UTC |           c |        3 |        3.3 |        false | 2016-01-28 00:00:00 UTC |
|       4 |       11,270 | 2015-01-29 11:54:36 UTC | 2015-01-29 00:00:00 UTC |           d |        4 |        4.4 |         true | 2016-01-29 00:00:00 UTC |
+---------+--------------+-------------------------+-------------------------+-------------+----------+------------+--------------+-------------------------+
```

## Todo

- Write Test
- Support default value

## Version

- 0.1.0: first release
- 0.1.1: bugfix
- 0.1.2: add confing option 'is_skip'
- 0.1.3: add failed log to exception msg

## Build

```
$ ./gradlew gem
```
