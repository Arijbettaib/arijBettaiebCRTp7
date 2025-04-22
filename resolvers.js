const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka } = require('kafkajs');

const movieProtoPath = './movie.proto';
const tvShowProtoPath = './tvShow.proto';
const movieProtoDefinition = protoLoader.loadSync(movieProtoPath, { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });
const tvShowProtoDefinition = protoLoader.loadSync(tvShowProtoPath, { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true });
const movieProto = grpc.loadPackageDefinition(movieProtoDefinition).movie;
const tvShowProto = grpc.loadPackageDefinition(tvShowProtoDefinition).tvShow;

const kafka = new Kafka({ clientId: 'gateway', brokers: ['localhost:9092'] });
const producer = kafka.producer();
producer.connect();

const resolvers = {
  Query: {
    movie: (_, { id }) => new Promise((resolve, reject) => {
      const client = new movieProto.MovieService('localhost:50051', grpc.credentials.createInsecure());
      client.GetMovie({ movie_id: id }, (err, response) => err ? reject(err) : resolve(response.movie));
    }),
    movies: () => new Promise((resolve, reject) => {
      const client = new movieProto.MovieService('localhost:50051', grpc.credentials.createInsecure());
      client.SearchMovies({ query: "" }, (err, response) => err ? reject(err) : resolve(response.movies));
    }),
    tvShow: (_, { id }) => new Promise((resolve, reject) => {
      const client = new tvShowProto.TVShowService('localhost:50052', grpc.credentials.createInsecure());
      client.GetTvshow({ tv_show_id: id }, (err, response) => err ? reject(err) : resolve(response.tv_show));
    }),
    tvShows: () => new Promise((resolve, reject) => {
      const client = new tvShowProto.TVShowService('localhost:50052', grpc.credentials.createInsecure());
      client.SearchTvshows({ query: "" }, (err, response) => err ? reject(err) : resolve(response.tv_shows));
    }),
  },
  Mutation: {
    createMovie: async (_, movie) => {
      await producer.send({ topic: 'movies_topic', messages: [{ value: JSON.stringify(movie) }] });
      return movie;
    },
    createTVShow: async (_, tvshow) => {
      await producer.send({ topic: 'tvshows_topic', messages: [{ value: JSON.stringify(tvshow) }] });
      return tvshow;
    }
  }
};

module.exports = resolvers;