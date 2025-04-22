// apiGateway.js
const express = require('express');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@apollo/server/express4');
const bodyParser = require('body-parser');
const cors = require('cors');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka } = require('kafkajs');

// Load proto files
const movieProtoPath = 'movie.proto';
const tvShowProtoPath = 'tvShow.proto';
const resolvers = require('./resolvers');
const typeDefs = require('./schema');

// Create Express app
const app = express();

// Load proto definitions
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

// Create Apollo Server
const server = new ApolloServer({ typeDefs, resolvers });

// Initialize server
const startServer = async () => {
  await server.start();
  
  // Apply middleware
  app.use(cors());
  app.use(bodyParser.json());
  app.use('/graphql', expressMiddleware(server));
  
  // REST endpoints
  app.get('/movies', (req, res) => {
    const client = new movieProto.MovieService(
      'localhost:50051',
      grpc.credentials.createInsecure()
    );
    client.searchMovies({}, (err, response) => {
      if (err) {
        res.status(500).send(err);
      } else {
        res.json(response.movies);
      }
    });
  });
  
  app.get('/movies/:id', (req, res) => {
    const client = new movieProto.MovieService(
      'localhost:50051',
      grpc.credentials.createInsecure()
    );
    const id = req.params.id;
    client.getMovie({ movieId: id }, (err, response) => {
      if (err) {
        res.status(500).send(err);
      } else {
        res.json(response.movie);
      }
    });
  });
  
  app.post('/movies', async (req, res) => {
    const client = new movieProto.MovieService(
      'localhost:50051',
      grpc.credentials.createInsecure()
    );
    const movieData = req.body;
    
    // Send message to Kafka
    await sendMessage('movies_topic', { type: 'CREATE', movie: movieData });
    
    // Format the movie data for gRPC
    const movieRequest = {
      movie: {
        title: movieData.title,
        description: movieData.description
      }
    };
    
    client.createMovie(movieRequest, (err, response) => {
      if (err) {
        console.error('Error creating movie:', err);
        res.status(500).send(err);
      } else {
        res.json(response.movie);
      }
    });
  });
  
  app.get('/tvshows', (req, res) => {
    const client = new tvShowProto.TVShowService(
      'localhost:50052',
      grpc.credentials.createInsecure()
    );
    client.searchTvshows({}, (err, response) => {
      if (err) {
        res.status(500).send(err);
      } else {
        res.json(response.tv_shows);
      }
    });
  });
  
  app.get('/tvshows/:id', (req, res) => {
    const client = new tvShowProto.TVShowService(
      'localhost:50052',
      grpc.credentials.createInsecure()
    );
    const id = req.params.id;
    client.getTvshow({ tvShowId: id }, (err, response) => {
      if (err) {
        res.status(500).send(err);
      } else {
        res.json(response.tv_show);
      }
    });
  });
  
  app.post('/tvshows', async (req, res) => {
    const client = new tvShowProto.TVShowService(
      'localhost:50052',
      grpc.credentials.createInsecure()
    );
    const tvShowData = req.body;
    
    // Send message to Kafka
    await sendMessage('tvshows_topic', { type: 'CREATE', tvShow: tvShowData });
    
    // Format the TV show data for gRPC
    const tvShowRequest = {
      tv_show: {
        title: tvShowData.title,
        description: tvShowData.description
      }
    };
    
    client.createTVShow(tvShowRequest, (err, response) => {
      if (err) {
        console.error('Error creating TV show:', err);
        res.status(500).send(err);
      } else {
        res.json(response.tv_show);
      }
    });
  });
  
  // Start Express server
  const port = 3000;
  app.listen(port, () => {
    console.log(`API Gateway running on port ${port}`);
  });
};

startServer().catch(err => {
  console.error('Error starting server:', err);
});