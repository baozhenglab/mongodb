package sdkmongo

import "go.mongodb.org/mongo-driver/mongo"

type MongoDBService interface {
	GetClient() *mongo.Client
	GetDatabase() *mongo.Database
}

type SyncMongo func(*mongo.Client) error

type mongodbService struct {
	client   *mongo.Client
	database *mongo.Database
}

func (s *mongodbService) GetClient() *mongo.Client {
	return s.client
}

func (s *mongodbService) GetDatabase() *mongo.Database {
	return s.database
}
