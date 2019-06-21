package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

var orderCount = 0

func main() {
	start := time.Now()

	orderCount, err := lineCounter()
	if err != nil {
		fmt.Println("Error counting inout record", err)
		return
	} else {
		fmt.Println("order records count is", orderCount)
	}

	//these channels are used for processing the extraction, transformation, and loading of the orders
	chExtract := make(chan *Order)
	chTransform := make(chan *Order, orderCount)
	chJobCompleted := make(chan bool)

	//the channels are used to keep a track of the number of items processed in each goroutine
	//they are used so we can identify when we can safely close the transform channel
	//and safely send the jobCompleted message to the Job Completed channel
	chTransformsCount := make(chan *Order, orderCount)
	chLoadCount := make(chan *Order, orderCount)

	//perform the ETL opeations - each
	go extract(chExtract)
	go transform(chExtract, chTransform, chTransformsCount)
	go load(chTransform, chJobCompleted, chLoadCount)

	//wait to receive on job completed channel
	<-chJobCompleted

	fmt.Println(time.Since(start))

}

func lineCounter() (int, error) {
	f, _ := os.Open("orders.txt")
	defer f.Close()

	r := io.Reader(f)
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func extract(chExtract chan *Order) {

	f, _ := os.Open("orders.txt")
	defer f.Close()
	r := csv.NewReader(f)

	for record, err := r.Read(); err == nil; record, err = r.Read() {
		order := new(Order)
		order.CustomerNumber, _ = strconv.Atoi(record[0])
		order.PartNumber = record[1]
		order.Quantity, _ = strconv.Atoi(record[2])
		chExtract <- order
	}

	fmt.Println("***** EXTRACT FINISHED - CLOSE EXTRACT CHANNEL")
	close(chExtract)

}

func transform(chExtract chan *Order, chTransform chan *Order, chTransformsCount chan *Order) {

	f, _ := os.Open("productList.txt")
	defer f.Close()
	r := csv.NewReader(f)

	records, _ := r.ReadAll()
	productList := make(map[string]*Product)

	for _, record := range records {
		product := new(Product)
		product.PartNumber = record[0]
		product.UnitCost, _ = strconv.ParseFloat(record[1], 64)
		product.UnitPrice, _ = strconv.ParseFloat(record[2], 64)
		productList[product.PartNumber] = product
	}

	for order := range chExtract {

		go func(order *Order) {
			time.Sleep(100 * time.Millisecond)
			product, _ := productList[order.PartNumber]
			order.UnitCost = product.UnitCost
			order.UnitPrice = product.UnitPrice
			//place the transformed order on the transformed order channel for pick up by the loader function
			chTransform <- order
			chTransformsCount <- order

		}(order)

	}

	for {
		var processed int = len(chTransformsCount)
		if processed == orderCount {
			fmt.Println("***** TRANSFORMS FINISHED - CLOSE TRANSFORM CHANNEL")
			close(chTransform)
			break
		} else {
			time.Sleep(1 * time.Millisecond)
		}
	}

}

func load(chTransform chan *Order, chJobCompleted chan bool, chLoadCount chan *Order) {

	f, _ := os.Create("dest.txt")
	defer f.Close()

	fmt.Fprintf(f, "%20s%15s%12s%12s%15s%15s\n", "Part Number", "Quantity", "Unit Cost", "Unit Price", "Total Cost", "Total Price")

	for o := range chTransform {

		go func(o *Order) {
			time.Sleep(100 * time.Millisecond)
			fmt.Fprintf(f, "%20s %15d %12.2f %12.2f %15.2f %15.2f\n",
				o.PartNumber, o.Quantity, o.UnitCost, o.UnitPrice, o.UnitCost*float64(o.Quantity), o.UnitPrice*float64(o.Quantity))
			//add to the counter channel so we know when we can close set the completed JOb Channel outside this func
			chLoadCount <- o
		}(o)
	}

	for {
		if processed := len(chLoadCount); processed == orderCount {
			fmt.Println("***** LOAD FINISHED - QUTTING PROGRAM")
			chJobCompleted <- true
			break
		} else {
			time.Sleep(1 * time.Millisecond)
		}
	}

}

type Product struct {
	PartNumber string
	UnitCost   float64
	UnitPrice  float64
}

type Order struct {
	CustomerNumber int
	PartNumber     string
	Quantity       int

	UnitCost  float64
	UnitPrice float64
}
