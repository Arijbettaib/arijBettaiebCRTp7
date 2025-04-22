const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka } = require('kafkajs');

const tvShowProtoPath = './tvShow.proto';
const tvShowProtoDefinition = protoLoader.loadSync(tvShowProtoPath, {
  keepCase: true, longs: String, enums: String, defaults: true, oneofs: true
});
const tvShowProto = grpc.loadPackageDefinition(tvShowProtoDefinition).tvShow;

const kafka = new Kafka({ clientId: 'tvshow-service', brokers: ['localhost:9092'] });
const producer = kafka.producer();
const topic = 'tvshows_topic';

const tvShowService = {
  getTvshow: (call, callback) => {
    const tv_show = {
      id: call.request.tv_show_id,
      title: 'Série TV',
      description: 'Exemple de série TV'
    };
    callback(null, { tv_show });
  },
  searchTvshows: (call, callback) => {
    const tv_shows = [
      { id: '1', title: 'TV Show 1', description: 'Exemple 1' },
      { id: '2', title: 'TV Show 2', description: 'Exemple 2' }
    ];
    callback(null, { tv_shows });
  }
};

const server = new grpc.Server();
server.addService(tvShowProto.TVShowService.service, tvShowService);
server.bindAsync("0.0.0.0:50052", grpc.ServerCredentials.createInsecure(), async (err, port) => {
  if (err) return console.error(err);
  await producer.connect();
  console.log("TV Show service running on port 50052");
  server.start();
});