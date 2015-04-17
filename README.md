# Lead Scoring Engine Template

## Documentation

Please refer to http://docs.prediction.io/templates/leadscoring/quickstart/

## Versions

### v0.3.0

- update for PredictionIO 0.9.2, including:

  - use new PEventStore API
  - use appName in DataSource parameter

### v0.2.0

- update build.sbt and template.json for PredictionIO 0.9.2

### v0.1.0

- initial release (require PredictionIO 0.9.0)


## Development Notes

### Sample Query

```
$ curl -H "Content-Type: application/json" \
-d '{
  "landingPageId" : "example.com/page9",
  "referrerId" : "referrer10.com",
  "browser": "Firefox" }' \
http://localhost:8000/queries.json \
-w %{time_total}
```

```
$ curl -H "Content-Type: application/json" \
-d '{
  "landingPageId" : "example.com/page9",
  "referrerId" : "referrer10.com",
  "browser": "Chrome" }' \
http://localhost:8000/queries.json \
-w %{time_total}
```

```
$ curl -H "Content-Type: application/json" \
-d '{
  "landingPageId" : "x",
  "referrerId" : "y",
  "browser": "z" }' \
http://localhost:8000/queries.json \
-w %{time_total}
```
