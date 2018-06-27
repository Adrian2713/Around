package main

import (
  "github.com/auth0/go-jwt-middleware"
  "github.com/dgrijalva/jwt-go"
  "github.com/gorilla/mux"

  "time"

  "context"
  "cloud.google.com/go/storage"

	elastic "gopkg.in/olivere/elastic.v3"
  "fmt"
  "net/http"
  "encoding/json"
  "io"
  "log"
	"strconv"
	"reflect"
  "github.com/pborman/uuid"



)

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}


type Post struct {
	User string `json:"user"`
	Message string `json:"message"`
	Location Location `json:"location"` 
  Url    string `json:"url"`
}
const(
	INDEX = "around"
	TYPE = "post"
	DISTANCE = "200km"
 
	ES_URL = "http://35.188.14.221:9200"
  BUCKET_NAME = "post-images-207823"
  PROJECT_ID = "around-207823"
  BT_INSTANCE = "around-post"

)

var mySigningKey = []byte("secret")

func main() {// Create a client
  client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
  if err != nil {
    panic(err)
    return
  }

  // Use the IndexExists service to check if a specified index exists.
  exists, err := client.IndexExists(INDEX).Do()
  if err != nil {
    panic(err)
  }
  if !exists {
    // Create a new index.
    mapping := `{
      "mappings":{
        "post":{
          "properties":{
            "location":{
              "type":"geo_point"
            }
          }
        }
      }
    }`
    _, err := client.CreateIndex(INDEX).Body(mapping).Do()
    if err != nil {
      // Handle error
      panic(err)
    }
  }

	fmt.Println("started-service")

  r := mux.NewRouter()

  var jwtMiddleware = jwtmiddleware.New(jwtmiddleware.Options{
         ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
                return mySigningKey, nil
         },
         SigningMethod: jwt.SigningMethodHS256,
  })

  r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST")
  r.Handle("/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearch))).Methods("GET")
  r.Handle("/login", http.HandlerFunc(loginHandler)).Methods("POST")
  r.Handle("/signup", http.HandlerFunc(signupHandler)).Methods("POST")

  http.Handle("/", r)
  log.Fatal(http.ListenAndServe(":8080", nil))
}

func handlerPost(w http.ResponseWriter, r *http.Request) {
      // Other codes
     w.Header().Set("Content-Type", "application/json")
     w.Header().Set("Access-Control-Allow-Origin", "*")
     w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

    user := r.Context().Value("user")
    claims := user.(*jwt.Token).Claims
    username := claims.(jwt.MapClaims)["username"]



      // 32 << 20 is the maxMemory param for ParseMultipartForm, equals to 32MB (1MB = 1024 * 1024 bytes = 2^20 bytes)
      // After you call ParseMultipartForm, the file will be saved in the server memory with maxMemory size.
      // If the file size is larger than maxMemory, the rest of the data will be saved in a system temporary file.
      r.ParseMultipartForm(32 << 20)

      // Parse from form data.
      fmt.Printf("Received one post request %s\n", r.FormValue("message"))
      lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
      lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)

      //part1 non-file data, for the savetoES
      p := &Post{
             User:    username.(string),
             Message: r.FormValue("message"),
             Location: Location{
                    Lat: lat,
                    Lon: lon,
             },
      }

      id := uuid.New()

      //part2 file data, for the savetoGCS
      file, _, err := r.FormFile("image")
      if err != nil {
             http.Error(w, "Image is not available", http.StatusInternalServerError)
             fmt.Printf("Image is not available %v.\n", err)
             return
      }
      defer file.Close()

      ctx := context.Background()

     //part2
     // replace it with your real bucket name.
      _, attrs, err := saveToGCS(ctx, file, BUCKET_NAME, id)
      if err != nil {
             http.Error(w, "GCS is not setup", http.StatusInternalServerError)
             fmt.Printf("GCS is not setup %v\n", err)
             return
      }

      // Update the media link after saving to GCS.
      p.Url = attrs.MediaLink

      //part 1
      // Save to ES.
      saveToES(p, id)





      //BT
      // Save to BigTable.
      //saveToBigTable(p, id)

      // you must update project name here
      bt_client, err := bigtable.NewClient(ctx, PROJECT_ID, BT_INSTANCE)
      if err != nil {
             panic(err)
             return
      }

      tbl := bt_client.Open("post")
      mut := bigtable.NewMutation()
      //get the time stamp
      t := bigtable.Now()

      mut.Set("post", "user", t, []byte(p.User))
      mut.Set("post", "message", t, []byte(p.Message))
      mut.Set("location", "lat", t, []byte(strconv.FormatFloat(p.Location.Lat, 'f', -1, 64)))
      mut.Set("location", "lon", t, []byte(strconv.FormatFloat(p.Location.Lon, 'f', -1, 64)))

      err = tbl.Apply(ctx, id, mut)
      if err != nil {
             panic(err)
             return
      }
      fmt.Printf("Post is saved to BigTable: %s\n", p.Message)

}

// Save an image to GCS.
func saveToGCS(ctx context.Context, r io.Reader, bucketName, name string) (*storage.ObjectHandle, *storage.ObjectAttrs, error) {
      client, err := storage.NewClient(ctx)
      if err != nil {
             return nil, nil, err
      }
      defer client.Close()

      bucket := client.Bucket(bucketName)
      // Next check if the bucket exists
      if _, err = bucket.Attrs(ctx); err != nil {
             return nil, nil, err
      }

      obj := bucket.Object(name)
      w := obj.NewWriter(ctx)
      if _, err := io.Copy(w, r); err != nil {
             return nil, nil, err
      }
      if err := w.Close(); err != nil {
             return nil, nil, err
      }

      //set control authority      
      if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
             return nil, nil, err
      }

      //get the url
      attrs, err := obj.Attrs(ctx)
      fmt.Printf("Post is saved to GCS: %s\n", attrs.MediaLink)
      return obj, attrs, err
}



// Save a post to ElasticSearch
func saveToES(p *Post, id string) {
  // Create a client
  es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
  if err != nil {
    panic(err)
    return
  }

  // Save it to index
  _, err = es_client.Index().
    Index(INDEX).
    Type(TYPE).  //！
    Id(id).
    BodyJson(p). //！
    Refresh(true).
    Do()
  if err != nil {
    panic(err)
    return
  }

  fmt.Printf("Post is saved to Index: %s\n", p.Message)
}


func handlerSearch(w http.ResponseWriter, r *http.Request) {
      	fmt.Println("Received one request for search")
      	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64)
      	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
      	// range is optional 
      	ran := DISTANCE 
      	if val := r.URL.Query().Get("range"); val != "" { 
        	 ran = val + "km" 
      	}

      	fmt.Println("range is ", ran)

        // Create a client
        client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
        if err != nil {
               panic(err)
               return
        }

        // Define geo distance query as specified in
        // https://www.elastic.co/guide/en/elasticsearch/reference/5.2/query-dsl-geo-distance-query.html
        q := elastic.NewGeoDistanceQuery("location")
        q = q.Distance(ran).Lat(lat).Lon(lon)

        // Some delay may range from seconds to minutes. So if you don't get enough results. Try it later.
        searchResult, err := client.Search().
               Index(INDEX).
               Query(q).
               Pretty(true).
               Do()
        if err != nil {
               // Handle error
               panic(err)
        }

        // searchResult is of type SearchResult and returns hits, suggestions,
        // and all kinds of other information from Elasticsearch.
        fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
        // TotalHits is another convenience function that works even when something goes wrong.
        fmt.Printf("Found a total of %d post\n", searchResult.TotalHits())

        // Each is a convenience function that iterates over hits in a search result.
        // It makes sure you don't need to check for nil values in the response.
        // However, it ignores errors in serialization.
        var typ Post
        var ps []Post
        for _, item := range searchResult.Each(reflect.TypeOf(typ)) { // instance of
               p := item.(Post) // p = (Post) item
               fmt.Printf("Post by %s: %s at lat %v and lon %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)
               // TODO(student homework): Perform filtering based on keywords such as web spam etc.
               ps = append(ps, p)

        }
        js, err := json.Marshal(ps)
        if err != nil {
               panic(err)
               return
        }

        w.Header().Set("Content-Type", "application/json")
        w.Header().Set("Access-Control-Allow-Origin", "*")
        w.Write(js)


} 


// checkUser checks whether user is valid
func checkUser(username, password string) bool {
      es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
      if err != nil {
             fmt.Printf("ES is not setup %v\n", err)
             return false
      }

      // Search with a term query
      termQuery := elastic.NewTermQuery("username", username)
      queryResult, err := es_client.Search().
             Index(INDEX).
             Query(termQuery).
             Pretty(true).
             Do()
      if err != nil {
             fmt.Printf("ES query failed %v\n", err)
             return false
      }

      var tyu User
      for _, item := range queryResult.Each(reflect.TypeOf(tyu)) {
             u := item.(User)
             return u.Password == password && u.Username == username
      }
      // If no user exist, return false.
      return false
}
// add user adds a new user

// Add a new user. Return true if successfully.
func addUser(user User) bool {
  es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
  if err != nil {
    fmt.Printf("ES is not setup %v\n", err)
    return false
  }

  termQuery := elastic.NewTermQuery("username", user.Username)
  queryResult, err := es_client.Search().
    Index(INDEX).
    Query(termQuery).
    Pretty(true).
    Do()
  if err != nil {
    fmt.Printf("ES query failed %v\n", err)
    return false
  }

  //number of such name user is larger than 1
  if queryResult.TotalHits() > 0 {
    fmt.Printf("User %s already exists, cannot create duplicate user.\n", user.Username)
    return false
  }

  _, err = es_client.Index().
    Index(INDEX).
    Type(TYPE_USER).
    Id(user.Username).
    BodyJson(user).
    Refresh(true).
    Do()
  if err != nil {
    fmt.Printf("ES save user failed %v\n", err)
    return false
  }

  return true
}

// If signup is successful, a new session is created


// If signup is successful, a new session is created.
func signupHandler(w http.ResponseWriter, r *http.Request) {
      fmt.Println("Received one signup request")

      decoder := json.NewDecoder(r.Body)
      var u User
      if err := decoder.Decode(&u); err != nil {
             panic(err)
             return
      }

      if u.Username != "" && u.Password != "" && usernamePattern(u.Username) {
             if addUser(u) {
                    fmt.Println("User added successfully.")
                    w.Write([]byte("User added successfully."))
             } else {
                    fmt.Println("Failed to add a new user.")
                    http.Error(w, "Failed to add a new user", http.StatusInternalServerError)
             }
      } else {
             fmt.Println("Empty password or username.")
             http.Error(w, "Empty password or username", http.StatusInternalServerError)
      }

      w.Header().Set("Content-Type", "text/plain")
      w.Header().Set("Access-Control-Allow-Origin", "*")
}

// If login is successful, a new token is created.


// If login is successful, a new token is created.
func loginHandler(w http.ResponseWriter, r *http.Request) {
      fmt.Println("Received one login request")

      decoder := json.NewDecoder(r.Body)
      var u User
      if err := decoder.Decode(&u); err != nil {
             panic(err)
             return
      }

      if checkUser(u.Username, u.Password) {
             token := jwt.New(jwt.SigningMethodHS256)
             claims := token.Claims.(jwt.MapClaims)
             /* Set token claims */
             claims["username"] = u.Username
             claims["exp"] = time.Now().Add(time.Hour * 24).Unix()

             /* Sign the token with our secret */
             tokenString, _ := token.SignedString(mySigningKey)

             /* Finally, write the token to the browser window */
             w.Write([]byte(tokenString))
      } else {
             fmt.Println("Invalid password or username.")
             http.Error(w, "Invalid password or username", http.StatusForbidden)
      }

      w.Header().Set("Content-Type", "text/plain")
      w.Header().Set("Access-Control-Allow-Origin", "*")
}




