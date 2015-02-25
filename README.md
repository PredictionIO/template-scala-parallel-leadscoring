# Lead Score Prediction Engine Template

## Documentation

Please refer to TODO

## Versions

### develop


### Query

normal:

```
$ curl -H "Content-Type: application/json" \
-d '{
  "landId" : "example.com/page9",
  "referralId" : "refferal9.com",
  "browser": "Firefox" }' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```


```
$ curl -H "Content-Type: application/json" \
-d '{
  "landId" : "x",
  "referralId" : "y",
  "browser": "z" }' \
http://localhost:8000/queries.json \
-w %{time_connect}:%{time_starttransfer}:%{time_total}
```
