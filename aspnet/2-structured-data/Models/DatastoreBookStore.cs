// Copyright(c) 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

using Google.Cloud.Datastore.V1;
using Google.Protobuf;
using System;
using System.Linq;
using Grpc.Core;
using HigherLogics.Google.Datastore;

namespace GoogleCloudSamples.Models
{
    public class DatastoreBookStore : IBookStore
    {
        private readonly string _projectId;
        private readonly DatastoreDb _db;

        const string emulatorHost = "localhost";
        const int emulatorPort = 8081;
        const string namespaceId = "";

        /// <summary>
        /// Create a new datastore-backed bookstore.
        /// </summary>
        /// <param name="projectId">Your Google Cloud project id</param>
        public DatastoreBookStore(string projectId)
        {
            _projectId = projectId;
            var client = DatastoreClient.Create(new Channel(emulatorHost, emulatorPort, ChannelCredentials.Insecure));
            _db = DatastoreDb.Create(_projectId, namespaceId, client);
        }

        // [START create]
        public void Create(Book book)
        {
            _db.Insert<Book>(book);
        }
        // [END create]

        public void Delete(long id)
        {
            _db.Delete(id.ToKey<Book>());
        }

        // [START list]
        public BookList List(int pageSize, string nextPageToken)
        {
            var query = _db.CreateQuery<Book>();
            query.Limit = pageSize;
            if (!string.IsNullOrWhiteSpace(nextPageToken))
                query.StartCursor = ByteString.FromBase64(nextPageToken);
            var results = _db.RunQuery(query);
            return new BookList()
            {
                Books = results.Entities<Book>(),
                NextPageToken = results.Entities.Count == query.Limit ?
                    results.EndCursor.ToBase64() : null
            };
        }
        // [END list]

        public Book Read(long id)
        {
            return _db.Lookup<Book>(id.ToKey<Book>());
        }

        public void Update(Book book)
        {
            _db.Update(book);
        }
    }
}