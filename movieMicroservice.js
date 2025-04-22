const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka } = require('kafkajs');

const movieProtoPath = './movie.proto';
const movieProtoDefinition = protoLoader.loadSync(movieProtoPath, {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true
});
const movieProto = grpc.loadPackageDefinition(movieProtoDefinition).movie;

const kafka = new Kafka({ clientId: 'movie-service', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const topic = 'movies_topic';

const movieService = {
  getMovie: (call, callback) => {
    const movie = {
      id: call.request.movie_id,
      title: 'Exemple de film',
      description: 'Ceci est un exemple de film.'
    };
    callback(null, { movie });
  },
  searchMovies: (call, callback) => {
    const movies = [
      { id: '1', title: 'Film 1', description: 'Film exemple 1' },
      { id: '2', title: 'Film 2', description: 'Film exemple 2' }
    ];
    callback(null, { movies });
  }
};

const server = new grpc.Server();
server.addService(movieProto.MovieService.service, movieService);
server.bindAsync("0.0.0.0:50051", grpc.ServerCredentials.createInsecure(), async (err, port) => {
  if (err) return console.error(err);
  await producer.connect();
  console.log("Movie service running on port 50051");
  server.start();
});