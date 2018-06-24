package main

import (
      // elastic "gopkg.in/olivere/elastic.v3"

      // "encoding/json"
      // "fmt"
      // "net/http"
      // "reflect"
      "regexp"
      // "time"

      // "github.com/dgrijalva/jwt-go"
)

const (
      TYPE_USER = "user"
)

var (
      //+ : one or more
      usernamePattern = regexp.MustCompile(`^[a-z0-9_]+$`).MatchString
)

type User struct {
      //`represents raw string which means there is no transferring meaning
      Username string `json:"username"`
      Password string `json:"password"`
      Age int `json:"age"`
      Gender string `json:"gender"`
}
