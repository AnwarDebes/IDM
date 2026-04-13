// ============================================================
// Epidemiological Data Warehouse — MongoDB Initialization
// ============================================================

db = db.getSiblingDB('epidemiological_dw');

// Create collections
db.createCollection('disease_observations');
db.createCollection('summary_monthly_by_region');
db.createCollection('summary_decade_national');

// Create indexes for disease_observations (bucket pattern)
db.disease_observations.createIndex(
    { "disease.name": 1, "time_bucket.year": 1, "location.state_code": 1 }
);
db.disease_observations.createIndex(
    { "location.region": 1, "time_bucket.decade": 1 }
);
db.disease_observations.createIndex(
    { "time_bucket.year": 1, "time_bucket.month": 1 }
);

// Create indexes for summary collections
db.summary_monthly_by_region.createIndex(
    { "_id.disease": 1, "_id.region": 1, "_id.year": 1, "_id.month": 1 }
);
db.summary_decade_national.createIndex(
    { "_id.disease": 1, "_id.decade": 1 }
);

print("MongoDB initialization complete: collections and indexes created.");
