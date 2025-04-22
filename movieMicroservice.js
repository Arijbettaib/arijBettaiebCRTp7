const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka } = require('kafkajs');
const sqlite3 = require('sqlite3').verbose();

// Load proto file
const movieProtoPath = 'movie.proto';
const movieProtoDefinition = protoLoader.loadSync(movieProtoPath, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const movieProto = grpc.loadPackageDefinition(movieProtoDefinition).movie;

// Initialize SQLite database
const db = new sqlite3.Database('./movies.db', (err) => {
  if (err) {
    console.error('Error opening database:', err);
    return;
  }
  console.log('Connected to SQLite database');
  
  // Create movies table if it doesn't exist
  db.run(`CREATE TABLE IF NOT EXISTS movies (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT
  )`, (err) => {
    if (err) {
      console.error('Error creating table:', err);
      return;
    }
    console.log('Movies table created or already exists');
    
    // Insert sample data if table is empty
    db.get("SELECT COUNT(*) as count FROM movies", (err, row) => {
      if (err) {
        console.error('Error checking table:', err);
        return;
      }
      
      if (row.count === 0) {
        // Insert sample movies
        const sampleMovies = [
          { id: '1', title: 'Example Movie 1', description: 'This is the first example movie.' },
          { id: '2', title: 'Example Movie 2', description: 'This is the second example movie.' }
        ];
        
        const stmt = db.prepare("INSERT INTO movies (id, title, description) VALUES (?, ?, ?)");
        sampleMovies.forEach(movie => {
          stmt.run(movie.id, movie.title, movie.description, (err) => {
            if (err) console.error('Error inserting sample movie:', err);
          });
        });
        stmt.finalize();
        console.log('Sample movies inserted');
      }
    });
  });
});

// Kafka configuration
const kafka = new Kafka({
  clientId: 'movie-microservice',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'movie-group' });

// Kafka consumer setup
const consumeMessages = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'movies_topic', fromBeginning: true });
  
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const data = JSON.parse(message.value.toString());
      console.log('Received message:', data);
      
      if (data.type === 'CREATE') {
        const newMovie = {
          id: String(Date.now()), // Use timestamp as ID
          ...data.movie
        };
        
        // Insert into database
        db.run(
          "INSERT INTO movies (id, title, description) VALUES (?, ?, ?)",
          [newMovie.id, newMovie.title, newMovie.description],
          (err) => {
            if (err) {
              console.error('Error inserting movie from Kafka:', err);
            } else {
              console.log('Created new movie from Kafka:', newMovie);
            }
          }
        );
      }
    },
  });
};

// Start Kafka consumer
consumeMessages().catch(console.error);

// Implement movie service
const movieService = {
  getMovie: (call, callback) => {
    const movieId = call.request.movie_id;
    
    db.get("SELECT * FROM movies WHERE id = ?", [movieId], (err, row) => {
      if (err) {
        callback(new Error('Database error'));
        return;
      }
      
      if (row) {
        callback(null, { movie: row });
      } else {
        callback(new Error('Movie not found'));
      }
    });
  },
  
  searchMovies: (call, callback) => {
    const { query } = call.request;
    
    if (query) {
      // Search with query
      db.all(
        "SELECT * FROM movies WHERE title LIKE ? OR description LIKE ?",
        [`%${query}%`, `%${query}%`],
        (err, rows) => {
          if (err) {
            callback(new Error('Database error'));
            return;
          }
          callback(null, { movies: rows });
        }
      );
    } else {
      // Get all movies
      db.all("SELECT * FROM movies", (err, rows) => {
        if (err) {
          callback(new Error('Database error'));
          return;
        }
        callback(null, { movies: rows });
      });
    }
  },
  
  createMovie: (call, callback) => {
    const movie = call.request.movie;
    const newMovie = {
      id: String(Date.now()), // Use timestamp as ID
      ...movie
    };
    
    console.log('Attempting to create movie:', newMovie);
    
    db.run(
      "INSERT INTO movies (id, title, description) VALUES (?, ?, ?)",
      [newMovie.id, newMovie.title, newMovie.description],
      (err) => {
        if (err) {
          console.error('Database error details:', err);
          callback({
            code: 2,
            message: 'Database error',
            details: err.message
          });
          return;
        }
        console.log('Successfully created movie:', newMovie);
        callback(null, { movie: newMovie });
      }
    );
  },
};

// Create and start gRPC server
const server = new grpc.Server();
server.addService(movieProto.MovieService.service, movieService);

const port = 50051;
server.bindAsync(
  `0.0.0.0:${port}`,
  grpc.ServerCredentials.createInsecure(),
  (err, port) => {
    if (err) {
      console.error('Failed to bind server:', err);
      return;
    }
    console.log(`Server running on port ${port}`);
    server.start();
  }
);

console.log(`Movie microservice running on port ${port}`);

// Close database connection when process exits
process.on('SIGINT', () => {
  db.close((err) => {
    if (err) {
      console.error('Error closing database:', err);
    } else {
      console.log('Database connection closed');
    }
    process.exit(0);
  });
});