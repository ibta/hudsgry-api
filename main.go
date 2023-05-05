package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/robfig/cron/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type MenuItem struct {
	Allergens                string `json:"Allergens"`
	Calories                 string `json:"Calories"`
	CaloriesFromFat          string `json:"Calories_From_Fat"`
	CateringDepartment       string `json:"Catering_Department"`
	Cholesterol              string `json:"Cholesterol"`
	CholesterolDv            string `json:"Cholesterol_DV"`
	DietaryFiber             string `json:"Dietary_Fiber"`
	DietaryFiberDv           string `json:"Dietary_Fiber_DV"`
	ID                       int    `json:"ID"`
	IngredientList           string `json:"Ingredient_List"`
	LocationName             string `json:"Location_Name"`
	LocationNumber           string `json:"Location_Number"`
	MealName                 string `json:"Meal_Name"`
	MealNumber               int    `json:"Meal_Number"`
	MenuCategoryName         string `json:"Menu_Category_Name"`
	MenuCategoryNumber       string `json:"Menu_Category_Number"`
	ProductionDepartment     string `json:"Production_Department"`
	Protein                  string `json:"Protein"`
	ProteinDv                string `json:"Protein_DV"`
	RecipeName               string `json:"Recipe_Name"`
	RecipeNumber             string `json:"Recipe_Number"`
	RecipePrintAsCharacter   string `json:"Recipe_Print_As_Character"`
	RecipePrintAsColor       string `json:"Recipe_Print_As_Color"`
	RecipePrintAsName        string `json:"Recipe_Print_As_Name"`
	RecipeProductInformation string `json:"Recipe_Product_Information"`
	RecipeWebCodes           string `json:"Recipe_Web_Codes"`
	SatFat                   string `json:"Sat_Fat"`
	SatFatDv                 string `json:"Sat_Fat_DV"`
	ServeDate                string `json:"Serve_Date"`
	ServiceDepartment        string `json:"Service_Department"`
	ServingSize              string `json:"Serving_Size"`
	Sodium                   string `json:"Sodium"`
	SodiumDv                 string `json:"Sodium_DV"`
	Sugars                   string `json:"Sugars"`
	SugarsDv                 string `json:"Sugars_DV"`
	TotalCarb                string `json:"Total_Carb"`
	TotalCarbDv              string `json:"Total_Carb_DV"`
	TotalFat                 string `json:"Total_Fat"`
	TotalFatDv               string `json:"Total_Fat_DV"`
	TransFat                 string `json:"Trans_Fat"`
	TransFatDv               string `json:"Trans_Fat_DV"`
	UpdateDate               string `json:"Update_Date"`
	PortionCost              string `json:"portion_cost"`
	SellingPrice             string `json:"selling_price"`
}

type CondensedMenuItem struct {
	Allergens     string  `json:"Allergens"`
	Calories      string  `json:"Calories"`
	FoodName      string  `json:"Food_Name"`
	HouseLocation bool    `json:"House_Location"`
	MealNumber    *int    `json:"Meal_Number,omitempty"`
	MenuCategory  string  `json:"Menu_Category_Name"`
	ServeDate     *string `json:"Serve_Date,omitempty"`
	Vegan         bool    `json:"Vegan"`
	Vegetarian    bool    `json:"Vegetarian"`
}

type CondensedMenu struct {
	ServeDate string              `json:"Serve_Date,omitempty"`
	Breakfast []CondensedMenuItem `json:"Breakfast"`
	Lunch     []CondensedMenuItem `json:"Lunch"`
	Dinner    []CondensedMenuItem `json:"Dinner"`
}

const apiUrl = "https://go.apis.huit.harvard.edu/ats/dining/v3/recipes"

var localCache = CondensedMenu{}

var client *mongo.Client
var collection *mongo.Collection

var earliestRecord string
var latestRecord string

var err error

func main() {

	// Init MongoDB client
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}

	uri := os.Getenv("MONGODB_URI")

	if uri == "" {
		log.Fatal("You must set your 'MONGODB_URI' environmental variable. See\n\t https://www.mongodb.com/docs/drivers/go/current/usage-examples/#environment-variable")
	}

	client, err = mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))

	if err != nil {
		panic(err)
	}
	defer func() {
		if err := client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	collection = client.Database("huds").Collection("data")
	collCount, err := collection.EstimatedDocumentCount(context.TODO())

	if err != nil {
		panic(err)
	}

	// Fetch data if there is no data in the database
	if collCount == 0 {
		log.Println("No data in database, fetching and processing data...")
		err := fetchAndProcessData()
		if err != nil {
			log.Printf("Failed to fetch HUDS data: %v\n", err)
		}
		log.Println("Fetched HUDS data successfully (in main)")
	}

	// Get earliest and latest records
	earliestRecord, latestRecord, err = getEarliestAndLatestRecords()
	if err != nil {
		log.Printf("Failed to get earliest and latest records: %v\n", err)
	}

	// Schedule data fetching and processing
	scheduler := cron.New(cron.WithLocation(time.FixedZone("EST", -5*60*60)))
	_, err = scheduler.AddFunc("0 3 * * *", func() {
		log.Println("Fetching and processing data...")
		err := fetchAndProcessData()
		if err != nil {
			log.Printf("Failed to fetch HUDS data: %v\n", err)
			return
		}
		log.Println("Fetched HUDS data successfully (in cron job)")
	})
	if err != nil {
		log.Fatalf("Failed to schedule data fetching and processing: %v", err)
	}
	scheduler.Start()

	router := gin.Default()

	router.GET("/huds-data", func(c *gin.Context) {
		serveDate := c.Query("serve_date")
		if serveDate == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "serve_date query parameter is required"})
			return
		}
		today := time.Now().Format("01/02/2006")

		// todo?? other sort of validation
		if today == serveDate && len(localCache.Dinner) > 0 {
			c.JSON(http.StatusOK, localCache)
			log.Println("Served from local cache")
			return
		} else {
			// Will set the local cache, so return here
			dbData, err := fetchDataByDate(serveDate)
			if err != nil || len(dbData.Dinner) == 0 {
				if err == mongo.ErrNoDocuments && (serveDate < earliestRecord) || (serveDate > latestRecord) {
					// Have some check if it is outside of the range of dates
					// Check if the date is before 05/05/2023 and return StatusNotFound if so
					// Otherwise, call fetchHUDSData() and return the result
					if serveDate < "05/05/2023" {
						c.JSON(http.StatusNotFound, gin.H{"error": "records don't exist before 05/05/2023 :("})
					} else {
						c.JSON(http.StatusNotFound, gin.H{"error": "date out of range"})
					}
					return
				}
				log.Println("dbData: ", dbData)
				log.Println("len dbData.Dinner: ", len(dbData.Dinner))
				log.Println("Failed to fetch data from MongoDB", err)
				log.Println("Failed to fetch data from MongoDB", err)
				log.Println("Failed to fetch data from MongoDB", err)
				log.Println("Failed to fetch data from MongoDB", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch data from MongoDB"})
				return
			}

			if today == serveDate {
				log.Println("Served from local cache")
				localCache = dbData
				c.JSON(http.StatusOK, localCache)
			}

			c.JSON(http.StatusOK, dbData)
			return
		}
	})

	err = router.Run(":8080")
	if err != nil {
		return
	}
}

func getEarliestAndLatestRecords() (string, string, error) {
	// Get the earliest and latest records from the database
	// If there are no records, return the earliest and latest dates that HUDS has data for

	// Cannot figure out why the database doesn't return a serve date, but improvising it for now
	filter := bson.D{}
	opts := options.FindOne().SetSort(bson.D{{"serve_date", 1}})
	var earliestRecord CondensedMenu
	var latestRecord CondensedMenu
	var earliestDate string
	var latestDate string
	err := collection.FindOne(context.TODO(), filter, opts).Decode(&earliestRecord)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			earliestDate = "05/05/2023"
		} else {
			return "", "", err
		}
	}

	opts2 := options.FindOne().SetSort(bson.D{{"serve_date", -1}})
	err = collection.FindOne(context.TODO(), filter, opts2).Decode(&latestRecord)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			latestDate = time.Now().Format("01/02/2006")
		} else {
			return "", "", err
		}
	}
	earliestDate = *earliestRecord.Breakfast[0].ServeDate
	latestDate = *latestRecord.Breakfast[0].ServeDate
	log.Println("earliestRecord: ", earliestDate)
	log.Println("latestRecord: ", latestDate)

	return earliestDate, latestDate, nil

}

func fetchAndProcessData() error {
	data, err := fetchHUDSData()
	if err != nil {
		log.Printf("Failed to fetch HUDS data: %v\n", err)
		return err
	}
	log.Println("Fetched HUDS data successfully")

	condensedData := ConvertMenuItemsToCondensedMenuItems(data)
	err = processDataAndStore(condensedData)
	if err != nil {
		log.Printf("Failed to process and store data: %v\n", err)
		return err
	}

	return nil
}

func fetchDataByDate(date string) (CondensedMenu, error) {
	//if err != nil {
	//	return CondensedMenu{}, fmt.Errorf("failed to get collection: %v", err)
	//}

	filter := bson.M{"serve_date": date}
	var result CondensedMenu
	err = collection.FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			// This error means your query did not match any documents.
			return CondensedMenu{}, mongo.ErrNoDocuments
		}
		panic(err)
	}
	log.Println("Found data in MongoDB")

	return result, nil
}

func processDataAndStore(data map[string]map[int][]CondensedMenuItem) error {
	// Store data in MongoDB
	updateOptions := options.Update().SetUpsert(true)
	currentDate := time.Now().Format("01/02/2006")

	if _, exists := data[currentDate]; exists {
		localCache.ServeDate, localCache.Breakfast, localCache.Lunch, localCache.Dinner = currentDate, data[currentDate][1], data[currentDate][2], data[currentDate][3]
	}

	for date, meals := range data {
		filter := bson.M{"serve_date": date}
		_, err = collection.UpdateOne(context.TODO(), filter, bson.D{{"$set", bson.D{
			{"serve_date", date},
			{"breakfast", meals[1]},
			{"lunch", meals[2]},
			{"dinner", meals[3]},
		}}}, updateOptions)
		if err != nil {
			log.Println("Failed to update data in MongoDB", err)
			return fmt.Errorf("failed to insert item into collection: %v", err)
		}
	}

	return nil
}

func ConvertToCondensedMenuItem(item MenuItem) (CondensedMenuItem, error) {
	// All of the houses have the same foods served, so we can just check one,
	// otherwise grab breakfast from Annenberg
	houseLocation := true
	if item.MealNumber == 1 && item.LocationName == "Annenberg Hall" {
		houseLocation = false
	} else if item.LocationName != "Currier House" || item.MealNumber == 1 && item.LocationName != "Annenberg Hall" {
		return CondensedMenuItem{}, fmt.Errorf("location not included: %s", item.LocationName)
	}

	return CondensedMenuItem{
		Allergens:     item.Allergens,
		Calories:      item.Calories,
		FoodName:      item.RecipePrintAsName,
		HouseLocation: houseLocation,
		MealNumber:    &item.MealNumber,
		MenuCategory:  item.MenuCategoryName,
		ServeDate:     &item.ServeDate,
		Vegan:         strings.Contains(item.RecipeWebCodes, "VGN"),
		Vegetarian:    strings.Contains(item.RecipeWebCodes, "VGT"),
	}, nil
}

func ConvertMenuItemsToCondensedMenuItems(items []MenuItem) map[string]map[int][]CondensedMenuItem {
	itemsByCategory := make(map[string]map[int][]CondensedMenuItem)

	for _, item := range items {
		condensedItem, err := ConvertToCondensedMenuItem(item)
		if err != nil {
			continue
		}
		key := *condensedItem.ServeDate
		mealNumber := *condensedItem.MealNumber

		if _, exists := itemsByCategory[key]; !exists {
			itemsByCategory[key] = make(map[int][]CondensedMenuItem)
		}

		// No longer needed, so remove from struct to save space
		condensedItem.ServeDate = nil
		condensedItem.MealNumber = nil

		if mealNumber == 1 {
			itemsByCategory[key][1] = append(itemsByCategory[key][1], condensedItem)
		} else if mealNumber == 2 && condensedItem.HouseLocation {
			itemsByCategory[key][2] = append(itemsByCategory[key][2], condensedItem)
		} else if mealNumber == 3 && condensedItem.HouseLocation {
			itemsByCategory[key][3] = append(itemsByCategory[key][3], condensedItem)
		}
	}

	return itemsByCategory
}

func fetchHUDSData() ([]MenuItem, error) {
	apiKey := os.Getenv("API_KEY")
	req, err := http.NewRequest("GET", apiUrl, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("x-api-key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			panic(err)
		}
	}(resp.Body)

	var data []MenuItem

	// Unmarshal the data response into the data struct
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		log.Fatal(err)
	}

	// log the first item of the data
	//log.Println(data[200])

	return data, err
}
