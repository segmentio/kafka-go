package kafka

import (
	"context"
	"fmt"
	"testing"
)

func TestControllerConn(t *testing.T){
    var c *Conn
    c, _ = DialLeader(context.Background(), "tcp", "localhost:9092", "topic", 0)
    var err  error
    c,err = c.GetControllerConn()
    if err!=nil{
        fmt.Println(err)
    }
}