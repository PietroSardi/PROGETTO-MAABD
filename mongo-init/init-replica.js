// replica set a 1 nodo. host=localhost così il driver dall'host Mac
// (che si connette a localhost:27017) vede il primary con lo stesso nome.
try {
  rs.status();
  print("replica set già attivo.");
} catch (e) {
  rs.initiate({
    _id: "rs0",
    members: [{ _id: 0, host: "localhost:27017" }]
  });
  print("replica set rs0 inizializzato.");
}
