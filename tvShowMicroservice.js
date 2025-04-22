// resolvers.js
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka } = require('kafkajs');

// Load proto files
const movieProtoPath = 'movie.proto';
const tvShowProtoPath = 'tvShow.proto';

// Kafka configuration
const kafka = new Kafka({
  clientId: 'api-gateway',
  brokers: ['localhost:9092']
});

const producer = kafka.producer();

// Kafka message sending function
const sendMessage = async (topic, message) => {
  await producer.connect();
  await producer.send({
    topic,
    messages: [{ value: JSON.stringify(message) }],
  });
  await producer.disconnect();
};

const movieProtoDefinition = protoLoader.loadSync(movieProtoPath, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const tvShowProtoDefinition = protoLoader.loadSync(tvShowProtoPath, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const movieProto = grpc.loadPackageDefinition(movieProtoDefinition).movie;
const tvShowProto = grpc.loadPackageDefinition(tvShowProtoDefinition).tvShow;

// Define resolvers
const resolvers = {
  Query: {
    movie: (_, { id }) => {
      const client = new movieProto.MovieService(
        'localhost:50051',
        grpc.credentials.createInsecure()
      );
      return new Promise((resolve, reject) => {
        client.getMovie({ movieId: id }, (err, response) => {
          if (err) {
            reject(err);
          } else {
            resolve(response.movie);
          }
        });
      });
    },
    movies: () => {
      const client = new movieProto.MovieService(
        'localhost:50051',
        grpc.credentials.createInsecure()
      );
      return new Promise((resolve, reject) => {
        client.searchMovies({}, (err, response) => {
          if (err) {
            reject(err);
          } else {
            resolve(response.movies);
          }
        });
      });
    },
    tvShow: (_, { id }) => {
      const client = new tvShowProto.TVShowService(
        'localhost:50052',
        grpc.credentials.createInsecure()
      );
      return new Promise((resolve, reject) => {
        client.getTvshow({ tvShowId: id }, (err, response) => {
          if (err) {
            reject(err);
          } else {
            resolve(response.tv_show);
          }
        });
      });
    },
    tvShows: () => {
      const client = new tvShowProto.TVShowService(
        'localhost:50052',
        grpc.credentials.createInsecure()
      );
      return new Promise((resolve, reject) => {
        client.searchTvshows({}, (err, response) => {
          if (err) {
            reject(err);
          } else {
            resolve(response.tv_shows);
          }
        });
      });
    },
  },
  Mutation: {
    createMovie: async (_, { title, description }) => {
      const client = new movieProto.MovieService(
        'localhost:50051',
        grpc.credentials.createInsecure()
      );
      const movie = { title, description };
      
      // Send message to Kafka
      await sendMessage('movies_topic', { type: 'CREATE', movie });
      
      return new Promise((resolve, reject) => {
        client.createMovie({ movie }, (err, response) => {
          if (err) reject(err);
          else resolve(response.movie);
        });
      });
    },
    createTVShow: async (_, { title, description }) => {
      const client = new tvShowProto.TVShowService(
        'localhost:50052',
        grpc.credentials.createInsecure()
      );
      const tvShow = { title, description };
      
      // Send message to Kafka
      await sendMessage('tvshows_topic', { type: 'CREATE', tvShow });
      
      return new Promise((resolve, reject) => {
        client.createTVShow({ tv_show: tvShow }, (err, response) => {
          if (err) reject(err);
          else resolve(response.tv_show);
        });
      });
    },
  },
};

module.exports = resolvers;