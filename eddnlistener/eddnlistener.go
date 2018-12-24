package main

import (
	"bytes"
	"compress/zlib"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"runtime"
	"strconv"
	"strings"
	"time"

	"../config"
	_ "github.com/mattn/go-sqlite3" // SQLite driver
	zmq "github.com/pebbe/zmq4"     // ZeroMQ
)

// Constants
var dataDir string = config.GetEnvWithDefault("EDDP_API_DATA_DIR", "./data")

var governments = struct {
	m map[string]string
}{m: map[string]string{
	"government_none":         "None",
	"government_anarchy":      "Anarchy",
	"government_colony":       "Colony",
	"government_communism":    "Communism",
	"government_confederacy":  "Confederacy",
	"government_cooperative":  "Cooperative",
	"government_corporate":    "Corporate",
	"government_democracy":    "Democracy",
	"government_dictatorship": "Dictatorship",
	"government_engineer":     "Engineer",
	"government_feudal":       "Feudal",
	"government_imperial":     "Imperial",
	"government_patronage":    "Patronage",
	"government_prisoncolony": "Prison Colony",
	"government_theocracy":    "Theocracy",
	"government_workshop":     "Workshop",
}}

var volcanisms = struct {
	m map[string]string
}{m: map[string]string{
	"ammonia":         "Ammonia",
	"carbon dioxide":  "Carbon Dioxide",
	"metallic":        "Iron",
	"methane":         "Methane",
	"nitrogen":        "Nitrogen",
	"rocky":           "Silicate",
	"silicate vapour": "Silicate vapour",
	"water":           "Water",
}}

var states = struct {
	m map[string]string
}{m: map[string]string{
	"none":        "None",
	"boom":        "Boom",
	"bust":        "Bust",
	"civilunrest": "Civil Unrest",
	"civilwar":    "Civil War",
	"election":    "Election",
	"expansion":   "Expansion",
	"famine":      "Famine",
	"investment":  "Investment",
	"lockdown":    "Lockdown",
	"outbreak":    "Outbreak",
	"retreat":     "Retreat",
	"war":         "War",
}}

var allegiances = struct {
	m map[string]string
}{m: map[string]string{
	"faction_none":        "None",
	"faction_federation":  "Federation",
	"faction_alliance":    "Alliance",
	"faction_empire":      "Empire",
	"faction_independent": "Independent",
	"faction_pirate":      "Pirate",
}}

var economies = struct {
	m map[string]string
}{m: map[string]string{
	"economy_none":         "None",
	"economy_agri":         "Agriculture",
	"economy_colony":       "Colony",
	"economy_extraction":   "Extraction",
	"economy_refinery":     "Refinery",
	"economy_industrial":   "Industrial",
	"economy_terraforming": "Terraforming",
	"economy_hightech":     "High Tech",
	"economy_service":      "Service",
	"economy_tourism":      "Tourism",
	"economy_military":     "Military",
}}

var securities = struct {
	m map[string]string
}{m: map[string]string{
	"system_security_none":          "None",
	"system_security_low":           "Low",
	"system_security_medium":        "Medium",
	"system_security_high":          "High",
	"system_security_high_anarchy":  "High",
	"galaxy_map_info_state_anarchy": "Anarchy",
	"galaxy_map_info_state_lawless": "Lawless",
}}

var materials = struct {
	m map[string]string
}{m: map[string]string{
	"carbon":     "Carbon",
	"iron":       "Iron",
	"nickel":     "Nickel",
	"phosphorus": "Phosphorus",
	"sulphur":    "Sulphur",
	"chromium":   "Chromium",
	"germanium":  "Germanium",
	"manganese":  "Manganese",
	"vanadium":   "Vanadium",
	"zinc":       "Zinc",
	"arsenic":    "Arsenic",
	"niobium":    "Niobium",
	"selenium":   "Selenium",
	"tungsten":   "Tungsten",
	"zirconium":  "Zirconium",
	"cadmium":    "Cadmium",
	"mercury":    "Mercury",
	"molybdenum": "Molybdenum",
	"tin":        "Tin",
	"yttrium":    "Yttrium",
	"antimony":   "Antimony",
	"polonium":   "Polonium",
	"ruthenium":  "Ruthenium",
	"technetium": "Technetium",
	"tellurium":  "Tellurium",
}}

var commodities = struct {
	m map[string]string
}{m: map[string]string{
	"agriculturalmedicines":         "Agri-Medicines",
	"animalmeat":                    "Animal Meat",
	"atmosphericextractors":         "Atmospheric Processors",
	"autofabricators":               "Auto-Fabricators",
	"basicnarcotics":                "Narcotics",
	"bioreducinglichen":             "Bioreducing Lichen",
	"bootlegliquor":                 "Legal Drugs",
	"cmmcomposite":                  "CMM Composite",
	"comercialsamples":              "Commercial Samples",
	"coolinghoses":                  "Micro-Weave Cooling Hoses",
	"drones":                        "Limpets",
	"encripteddatastorage":          "Encrypted Data Storage",
	"fruitandvegetables":            "Fruit and Vegetables",
	"hafnium178":                    "Hafnium 178",
	"hazardousenvironmentsuits":     "H.E. Suits",
	"heliostaticfurnaces":           "Microbial Furnaces",
	"hnshockmount":                  "HN Shock Mount",
	"marinesupplies":                "Marine Equipment",
	"metaalloys":                    "Meta-Alloys",
	"mutomimager":                   "Muon Imager",
	"nonlethalweapons":              "Non-lethal Weapons",
	"sap8corecontainer":             "Sap 8 Core Container",
	"skimercomponents":              "Skimmer Components",
	"terrainenrichmentsystems":      "Land Enrichment Systems",
	"trinketsoffortune":             "Trinkets of Hidden Fortune",
	"unkownartifact":                "Unknown Artefact",
	"usscargoancientartefact":       "Ancient Artefact",
	"usscargoexperimentalchemicals": "Experimental Chemicals",
	"usscargomilitaryplans":         "Military Plans",
	"usscargoprototypetech":         "Prototype Tech",
	"usscargorebeltransmissions":    "Rebel Transmissions",
	"usscargotechnicalblueprints":   "Technical Blueprints",
	"usscargotradedata":             "Trade Data",
	"wreckagecomponents":            "Salvageable Wreckage",
}}

var commodityids = struct {
	m map[string]int
}{m: map[string]int{
	"Explosives":                    1,
	"Hydrogen Fuel":                 2,
	"Mineral Oil":                   3,
	"Pesticides":                    4,
	"Clothing":                      5,
	"Consumer Technology":           6,
	"Domestic Appliances":           7,
	"Beer":                          8,
	"Liquor":                        9,
	"Narcotics":                     10,
	"Tobacco":                       11,
	"Wine":                          12,
	"Algae":                         13,
	"Animal Meat":                   14,
	"Coffee":                        15,
	"Fish":                          16,
	"Food Cartridges":               17,
	"Fruit and Vegetables":          18,
	"Grain":                         19,
	"Synthetic Meat":                20,
	"Tea":                           21,
	"Polymers":                      22,
	"Semiconductors":                23,
	"Superconductors":               24,
	"Atmospheric Processors":        25,
	"Crop Harvesters":               26,
	"Marine Equipment":              27,
	"Microbial Furnaces":            28,
	"Mineral Extractors":            29,
	"Power Generators":              30,
	"Water Purifiers":               31,
	"Agri-Medicines":                32,
	"Basic Medicines":               33,
	"Combat Stabilisers":            34,
	"Performance Enhancers":         35,
	"Progenitor Cells":              36,
	"Aluminium":                     37,
	"Beryllium":                     38,
	"Cobalt":                        39,
	"Copper":                        40,
	"Gallium":                       41,
	"Gold":                          42,
	"Indium":                        43,
	"Lithium":                       44,
	"Palladium":                     45,
	"Platinum":                      46,
	"Silver":                        47,
	"Tantalum":                      48,
	"Titanium":                      49,
	"Uranium":                       50,
	"Bauxite":                       51,
	"Bertrandite":                   52,
	"Coltan":                        53,
	"Gallite":                       54,
	"Indite":                        55,
	"Lepidolite":                    56,
	"Rutile":                        57,
	"Uraninite":                     58,
	"Imperial Slaves":               59,
	"Slaves":                        60,
	"Advanced Catalysers":           61,
	"Animal Monitors":               62,
	"Aquaponic Systems":             63,
	"Auto-Fabricators":              64,
	"Bioreducing Lichen":            65,
	"Computer Components":           66,
	"H.E. Suits":                    67,
	"Land Enrichment Systems":       68,
	"Resonating Separators":         69,
	"Robotics":                      70,
	"Leather":                       72,
	"Natural Fabrics":               73,
	"Synthetic Fabrics":             74,
	"Biowaste":                      75,
	"Chemical Waste":                76,
	"Scrap":                         77,
	"Non-lethal Weapons":            78,
	"Personal Weapons":              79,
	"Reactive Armour":               80,
	"Battle Weapons":                81,
	"Toxic Waste":                   82,
	"Painite":                       83,
	"Limpet":                        84,
	"Eranin Pearl Whiskey":          85,
	"Kamorin Historic Weapons":      86,
	"Lucan Onion Head":              87,
	"Motrona Experience Jelly":      88,
	"Onion Head":                    89,
	"Rusani Old Smokey":             90,
	"Tarach Spice":                  91,
	"Terra Mater Blood Bores":       92,
	"Wolf Fesh":                     93,
	"Wuthielo Ku Froth":             94,
	"Ai Relics":                     95,
	"Antiquities":                   96,
	"Osmium":                        97,
	"Sap 8 Core Container":          98,
	"Trinkets Of Hidden Fortune":    99,
	"Trade Data":                    100,
	"Thermal Cooling Units":         101,
	"Skimmer Components":            102,
	"Geological Equipment":          103,
	"Structural Regulators":         104,
	"Pyrophyllite":                  105,
	"Moissanite":                    106,
	"Goslarite":                     107,
	"Cryolite":                      108,
	"Thorium":                       109,
	"Thallium":                      110,
	"Lanthanum":                     111,
	"Bismuth":                       112,
	"Bootleg Liquor":                113,
	"Meta-Alloys":                   114,
	"Ceramic Composites":            115,
	"Evacuation Shelter":            116,
	"Synthetic Reagents":            117,
	"Surface Stabilisers":           118,
	"Building Fabricators":          119,
	"Landmines":                     121,
	"Nerve Agents":                  122,
	"Occupied CryoPod":              123,
	"Muon Imager":                   124,
	"Lavian Brandy":                 125,
	"Black Box":                     126,
	"Military Plans":                127,
	"Ancient Artefact":              128,
	"Rare Artwork":                  129,
	"Experimental Chemicals":        130,
	"Rebel Transmissions":           131,
	"Prototype Tech":                132,
	"Technical Blueprints":          133,
	"HIP 10175 Bush Meat":           134,
	"Albino Quechua Mammoth":        135,
	"Utgaroar Millennial Eggs":      136,
	"Witchhaul Kobe Beef":           137,
	"Karsuki Locusts":               138,
	"Giant Irukama Snails":          139,
	"Baltah Sine Vacuum Krill":      140,
	"Ceti Rabbits":                  141,
	"Kachirigin Filter Leeches":     142,
	"Lyrae Weed":                    143,
	"Borasetani Pathogenetics":      144,
	"HIP 118311 Swarm":              145,
	"Kongga Ale":                    146,
	"Alacarakmo Skin Art":           147,
	"Eleu Thermals":                 148,
	"Eshu Umbrellas":                149,
	"Karetii Couture":               150,
	"Njangari Saddles":              151,
	"Any Na Coffee":                 152,
	"CD-75 Kitten Brand Coffee":     153,
	"Goman Yaupon Coffee":           154,
	"Volkhab Bee Drones":            155,
	"Kinago Violins":                156,
	"Nguna Modern Antiques":         157,
	"Rajukru Multi-Stoves":          158,
	"Tiolce Waste2Paste Units":      159,
	"Chi Eridani Marine Paste":      160,
	"Esuseku Caviar":                161,
	"Live Hecate Sea Worms":         162,
	"Helvetitj Pearls":              163,
	"HIP Proto-Squid":               164,
	"Coquim Spongiform Victuals":    165,
	"Eden Apples Of Aerial":         166,
	"Neritus Berries":               167,
	"Ochoeng Chillies":              168,
	"Deuringas Truffles":            169,
	"HR 7221 Wheat":                 170,
	"Jaroua Rice":                   171,
	"Belalans Ray Leather":          172,
	"Damna Carapaces":               173,
	"Rapa Bao Snake Skins":          174,
	"Vanayequi Ceratomorpha Fur":    175,
	"Bast Snake Gin":                176,
	"Thrutis Cream":                 177,
	"Wulpa Hyperbore Systems":       178,
	"Aganippe Rush":                 179,
	"Holva Duelling Blades":         180,
	"Gilya Signature Weapons":       181,
	"Delta Phoenicis Palms":         182,
	"Toxandji Virocide":             183,
	"Xihe Biomorphic Companions":    184,
	"Sanuma Decorative Meat":        185,
	"Ethgreze Tea Buds":             186,
	"Ceremonial Heike Tea":          187,
	"Tanmark Tranquil Tea":          188,
	"Az Cancri Formula 42":          189,
	"Kamitra Cigars":                190,
	"Yaso Kondi Leaf":               191,
	"Chateau De Aegaeon":            192,
	"Waters Of Shintara":            193,
	"Ophiuch Exino Artefacts":       194,
	"Aepyornis Egg":                 195,
	"Saxon Wine":                    196,
	"Centauri Mega Gin":             197,
	"Anduliga Fire Works":           198,
	"Banki Amphibious Leather":      199,
	"Cherbones Blood Crystals":      200,
	"Geawen Dance Dust":             201,
	"Gerasian Gueuze Beer":          202,
	"Haidne Black Brew":             203,
	"Havasupai Dream Catcher":       204,
	"Burnham Bile Distillate":       205,
	"HIP Organophosphates":          206,
	"Jaradharre Puzzle Box":         207,
	"Koro Kung Pellets":             208,
	"Void Extract Coffee":           209,
	"Honesty Pills":                 210,
	"Non Euclidian Exotanks":        211,
	"LTT Hypersweet":                212,
	"Mechucos High Tea":             213,
	"Medb Starlube":                 214,
	"Mokojing Beast Feast":          215,
	"Mukusubii Chitin-Os":           216,
	"Mulachi Giant Fungus":          217,
	"Ngadandari Fire Opals":         218,
	"Tiegfries Synth Silk":          219,
	"Uzumoku Low-G Wings":           220,
	"V Herculis Body Rub":           221,
	"Wheemete Wheat Cakes":          222,
	"Vega Slimweed":                 223,
	"Altairian Skin":                224,
	"Pavonis Ear Grubs":             225,
	"Jotun Mookah":                  226,
	"Giant Verrix":                  227,
	"Indi Bourbon":                  228,
	"Arouca Conventual Sweets":      229,
	"Tauri Chimes":                  230,
	"Zeessze Ant Grub Glue":         231,
	"Pantaa Prayer Sticks":          232,
	"Fujin Tea":                     233,
	"Chameleon Cloth":               234,
	"Orrerian Vicious Brew":         235,
	"Uszaian Tree Grub":             236,
	"Momus Bog Spaniel":             237,
	"Diso Ma Corn":                  238,
	"Leestian Evil Juice":           239,
	"Azure Milk":                    240,
	"Leathery Eggs":                 241,
	"Alya Body Soap":                242,
	"Vidavantian Lace":              243,
	"Jaques Quinentian Still":       244,
	"Soontill Relics":               245,
	"Unknown Artefact":              246,
	"Hafnium 178":                   247,
	"Military Intelligence":         248,
	"The Hutton Mug":                249,
	"Sothis Crystalline Gold":       250,
	"Salvageable Wreckage":          251,
	"Encrypted Data Storage":        252,
	"Personal Effects":              253,
	"Commercial Samples":            254,
	"Tactical Data":                 255,
	"Assault Plans":                 256,
	"Encrypted Correspondence":      257,
	"Diplomatic Bag":                258,
	"Scientific Research":           259,
	"Scientific Samples":            260,
	"Political Prisoner":            261,
	"Hostage":                       262,
	"Geological Samples":            263,
	"Master Chefs":                  264,
	"Crystalline Spheres":           265,
	"Taaffeite":                     266,
	"Jadeite":                       267,
	"Unstable Data Core":            268,
	"Onionhead Alpha Strain":        269,
	"Occupied Escape Pod":           270,
	"Water":                         271,
	"Onionhead Beta Strain":         272,
	"Praseodymium":                  273,
	"Bromellite":                    274,
	"Samarium":                      275,
	"Low Temperature Diamonds":      276,
	"Hydrogen Peroxide":             277,
	"Liquid Oxygen":                 278,
	"Methanol Monohydrate":          279,
	"Lithium Hydroxide":             280,
	"Methane Clathrate":             281,
	"Insulating Membrane":           282,
	"CMM Composite":                 283,
	"Micro-Weave Cooling Hoses":     284,
	"Neofabric Insulation":          285,
	"Articulation Motors":           286,
	"HN Shock Mount":                287,
	"Emergency Power Cells":         288,
	"Power Converter":               289,
	"Energy Grid Assembly":          290,
	"Power Transfer Bus":            291,
	"Radiation Baffle":              292,
	"Exhaust Manifold":              293,
	"Reinforced Mounting Plate":     294,
	"Heatsink Interlink":            295,
	"Magnetic Emitter Coil":         296,
	"Modular Terminals":             297,
	"Nanobreakers":                  298,
	"Telemetry Suite":               299,
	"Micro Controllers":             300,
	"Ion Distributor":               301,
	"Hardware Diagnostic Sensor":    302,
	"Conductive Fabrics":            303,
	"Military Grade Fabrics":        304,
	"Advanced Medicines":            305,
	"Medical Diagnostic Equipment":  306,
	"Survival Equipment":            307,
	"Data Core":                     308,
	"Galactic Travel Guide":         309,
	"Mysterious Idol":               310,
	"Prohibited Research Materials": 311,
	"Antimatter Containment Unit":   312,
	"Space Pioneer Relics":          313,
	"Fossil Remnants":               314,
	"Unknown Probe":                 315,
	"Precious Gems":                 316,
}}

// Database connections
var eddpDb *sql.DB

type Systems struct {
	System []struct {
		data map[string]interface{}
	}
}

func assertNil(e error) {
	if e != nil {
		log.Print(e)
		panic(e)
	}
}

func errFound(e error, msg string) bool {
	if e != nil {
		log.Print(e)
		log.Print(msg)
		return true
	}
	return false
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	for {
		var err error
		eddpDb, err = sql.Open("sqlite3", dataDir+"/sqlite/eddp.sqlite")
		if err != nil {
			log.Print(err)
		}
		defer eddpDb.Close()

		subscriber, _ := zmq.NewSocket(zmq.SUB)
		defer subscriber.Close()
		subscriber.Connect("tcp://eddn.edcd.io:9500")
		subscriber.SetSubscribe("")

		publisher, _ := zmq.NewSocket(zmq.PUB)
		publisher.Bind("tcp://*:5556")
		defer publisher.Close()

		for {
			raw, err := subscriber.RecvMessageBytes(0)
			if err != nil {
				break
			}
			var msg bytes.Buffer
			r, err := zlib.NewReader(bytes.NewReader(raw[0]))
			io.Copy(&msg, r)
			r.Close()
			HandleMessage(&msg, publisher)
		}
	}
}

func HandleMessage(msg *bytes.Buffer, publisher *zmq.Socket) {
	// Turn the message in to JSON
	d := json.NewDecoder(strings.NewReader(msg.String()))
	d.UseNumber()
	var data map[string]interface{}
	err := d.Decode(&data)
	if errFound(err, msg.String()) {
		return
	}

	// Check to see if it is interesting to us and not blocked
	clientName := data["header"].(map[string]interface{})["softwareName"].(string)
	clientVersion := data["header"].(map[string]interface{})["softwareVersion"].(string)
	if !ClientAllowed(clientName, clientVersion) {
		return
	}

	schema := data["$schemaRef"].(string)
	if schema == "https://eddn.edcd.io/schemas/journal/1" {
		event := data["message"].(map[string]interface{})["event"].(string)
		if event == "FSDJump" {
			HandleFSDJumpEvent(msg.String(), data["message"].(map[string]interface{}), publisher)
		} else if event == "Docked" {
			HandleDockedEvent(msg.String(), data["message"].(map[string]interface{}), publisher)
		} else if event == "Scan" {
			stellarMass := data["message"].(map[string]interface{})["StellarMass"]
			if stellarMass == nil {
				HandleBodyScanEvent(msg.String(), data["message"].(map[string]interface{}), publisher)
			} else {
				HandleStarScanEvent(msg.String(), data["message"].(map[string]interface{}), publisher)
			}
		}
	} else if schema == "https://eddn.edcd.io/schemas/commodity/3" {
		HandleCommodity3Schema(msg.String(), data["message"].(map[string]interface{}), publisher)
	} else if schema == "https://eddn.edcd.io/schemas/outfitting/2" {
		HandleOutfitting2Schema(msg.String(), data["message"].(map[string]interface{}), publisher)
	}
}

func ClientAllowed(client string, version string) bool {
	// ED-IBE sends bad timestamps (e.g. 2017-01-17T20:43:33+01:00Z) so we can't use it
	if client == "ED-IBE (API)" {
		return false
	}

	// EDCE sends bad timestamps (e.g. 2017-01-18T15:04:04.582385) so we can't use it
	if client == "EDCE" {
		return false
	}

	// Elite G19s Companion App sends bad timestamps (e.g. 2017-01-20T09:19:13) so we can't use it
	if client == "Elite G19s Companion App" {
		return false
	}

	// EVA [iPhone] sends bad timestamps (e.g 2017-01-19T10:53:28 pmZ) so we can't use it
	if client == "EVA [iPhone]" {
		return false
	}

	// EVA [iPad] sends bad timestamps (e.g 2017-01-19T10:53:28 pmZ) so we can't use it
	if client == "EVA [iPad]" {
		return false
	}

	return true
}

func HandleBodyScanEvent(raw string, event map[string]interface{}, publisher *zmq.Socket) {
	systemname := event["StarSystem"].(string)
	bodyname := event["BodyName"].(string)

	// Fetch the current system from the database
	systemx, err := Float(event["StarPos"].([]interface{})[0])
	assertNil(err)
	systemx = fixCoord(systemx)
	systemy, err := Float(event["StarPos"].([]interface{})[1])
	assertNil(err)
	systemy = fixCoord(systemy)
	systemz, err := Float(event["StarPos"].([]interface{})[2])
	assertNil(err)
	systemz = fixCoord(systemz)

	systemdata, err := FetchSystem(systemname, systemx, systemy, systemz)
	if err != nil {
		// System doesn't exist - ignore
	} else {
		// Turn the system in to JSON
		d := json.NewDecoder(strings.NewReader(systemdata))
		d.UseNumber()
		var system map[string]interface{}
		err = d.Decode(&system)
		if errFound(err, raw) {
			return
		}

		// Now fetch the body
		systemId, err := Int(system["id"])
		if errFound(err, raw) {
			return
		}
		body, err := FetchBody(systemId, bodyname)
		var exists bool
		if err != nil {
			exists = false
			body = make(map[string]interface{})
			body["created_at"] = int32(time.Now().Unix())
		} else {
			exists = true
		}

		body["updated_at"] = int32(time.Now().Unix())

		// Periapsis
		periapsis, err := Float(event["Periapsis"])
		if err == nil {
			body["arg_of_periapsis"] = periapsis
		}
		// Distance
		distance, err := Float(event["DistanceFromArrivalLS"])
		if err == nil {
			body["distance_to_arrival"] = distance
		}
		// Eccentricity
		eccentricity, err := Float(event["Eccentricity"])
		if err == nil {
			body["orbital_eccentricity"] = eccentricity
		}
		// Mass
		mass, err := Float(event["MassEM"])
		if err == nil {
			body["earth_masses"] = mass
		}
		// Gravity
		gravity, err := Float(event["Gravity"])
		if err == nil {
			body["gravity"] = gravity / 9.80665
		}
		body["group_id"] = 6
		body["group_name"] = "Planet"
		body["is_landable"] = event["Landable"]
		body["is_rotational_period_tidally_locked"] = event["TidalLock"]
		// Materials
		if event["Materials"] != nil {
			switch v := event["Materials"].(type) {
			case []interface{}:
				materials := event["Materials"].([]interface{})
				// Build materials
				var materialsJson []map[string]interface{}
				materialsJson = make([]map[string]interface{}, len(materials))
				// transform each
				for i := range materials {
					var materialJson map[string]interface{}
					materialJson = make(map[string]interface{})

					material := materials[i].(map[string]interface{})
					materialJson["material_name"] = TranslateMaterial(material["Name"].(string))
					share, err := Float(material["Percent"])
					if err == nil {
						materialJson["share"] = share
					}
					materialsJson[i] = materialJson
				}
				body["materials"] = materialsJson
			default:
				log.Print("Unhandled materials type ", v, "; event is ", event)
			}
		}
		body["name"] = bodyname
		// Orbital inclination
		inclination, err := Float(event["OrbitalInclination"])
		if err == nil {
			body["orbital_inclination"] = inclination
		}
		// Orbital period
		orbitalPeriod, err := Float(event["OrbitalPeriod"])
		if err == nil {
			body["orbital_period"] = orbitalPeriod
		}
		// Radius
		radius, err := Float(event["Radius"])
		if err == nil {
			body["radius"] = radius / 1000
		}
		// Rotational period
		rotationalPeriod, err := Float(event["RotationPeriod"])
		if err == nil {
			body["rotational_period"] = rotationalPeriod / 86400
		}
		// Semi-major axis
		semiMajorAxis, err := Float(event["SemiMajorAxis"])
		if err == nil {
			body["semi_major_axis"] = semiMajorAxis / 149597870700
		}
		// Surface pressure
		pressure, err := Float(event["SurfacePressure"])
		if err == nil {
			body["surface_pressure"] = pressure / 101325
		}
		// Surface temperature
		surfaceTemperature, err := Float(event["SurfaceTemperature"])
		if err == nil {
			body["surface_temperature"] = surfaceTemperature
		}
		// Terraforming state
		terraformState := event["TerraformState"]
		if terraformState == "" {
			body["terraforming_state_id"] = 1
			body["terraforming_state_name"] = "Not terraformable"
		} else if terraformState == "Terraformable" {
			body["terraforming_state_id"] = 2
			body["terraforming_state_name"] = "Candidate for terraforming"
		} else if terraformState == "Terraforming" {
			body["terraforming_state_id"] = 3
			body["terraforming_state_name"] = "Terraforming completed"
		} else if terraformState == "Terraformed" {
			body["terraforming_state_id"] = 4
			body["terraforming_state_name"] = "Being terraformed"
		}
		// Type
		planetClass := event["PlanetClass"]
		if planetClass == "Sudarsky class I gas giant" {
			body["type_id"] = 21
			body["type"] = "Class I gas giant"
		} else if planetClass == "Sudarsky class II gas giant" {
			body["type_id"] = 22
			body["type"] = "Class II gas giant"
		} else if planetClass == "Sudarsky class III gas giant" {
			body["type_id"] = 23
			body["type"] = "Class III gas giant"
		} else if planetClass == "Sudarsky class IV gas giant" {
			body["type_id"] = 24
			body["type"] = "Class IV gas giant"
		} else if planetClass == "Sudarsky class V gas giant" {
			body["type_id"] = 25
			body["type"] = "Class V gas giant"
		} else if planetClass == "Earthlike body" {
			body["type_id"] = 26
			body["type"] = "Earth-like world"
		} else if planetClass == "Gas giant with ammonia based life" {
			body["type_id"] = 27
			body["type"] = "Gas giant with ammonia-based life"
		} else if planetClass == "Gas giant with water based life" {
			body["type_id"] = 28
			body["type"] = "Gas giant with water-based life"
		} else if planetClass == "Helium rich gas giant" {
			body["type_id"] = 29
			body["type"] = "Helium-rich gas giant"
		} else if planetClass == "High metal content body" {
			body["type_id"] = 30
			body["type"] = "High metal content world"
		} else if planetClass == "Icy body" {
			body["type_id"] = 31
			body["type"] = "Icy body"
		} else if planetClass == "Metal rich body" {
			body["type_id"] = 32
			body["type"] = "Metal-rich body"
		} else if planetClass == "Rocky body" {
			body["type_id"] = 33
			body["type"] = "Rocky body"
		} else if planetClass == "Rocky ice body" {
			body["type_id"] = 34
			body["type"] = "Rocky ice world"
		} else if planetClass == "Water giant" {
			body["type_id"] = 35
			body["type"] = "Water giant"
		} else if planetClass == "Water world" {
			body["type_id"] = 36
			body["type"] = "Water world"
		}
		if event["Volcanism"] != nil && event["Volcanism"] != "" && event["Volcanism"] != "No volcanism" {
			volcanism := event["Volcanism"].(string)
			volcanismJson := make(map[string]interface{})
			volcanism = strings.Replace(volcanism, " volcanism", "", 1)
			// Volcanism type
			if strings.HasSuffix(volcanism, " geysers") {
				volcanism = strings.Replace(volcanism, " geysers", "", 1)
				volcanismJson["type"] = "Geysers"
			} else if strings.HasSuffix(volcanism, " magma") {
				volcanism = strings.Replace(volcanism, " magma", "", 1)
				volcanismJson["type"] = "Magma"
			}
			// Volcanism amount
			if strings.HasPrefix(volcanism, "major") {
				volcanism = strings.Replace(volcanism, "major ", "", 1)
				volcanismJson["amount"] = "Major"
			} else if strings.HasPrefix(volcanism, "minor") {
				volcanism = strings.Replace(volcanism, "minor ", "", 1)
				volcanismJson["amount"] = "Minor"
			}
			// Volcanism composition
			volcanismJson["composition"] = TranslateVolcanism(volcanism)
			body["volcanism"] = volcanismJson
		}

		// Create or update
		bodystr, err := json.Marshal(body)
		if errFound(err, raw) {
			return
		}

		if exists {
			bodyId, err := Int(body["id"])
			if errFound(err, raw) {
				return
			}
			err = UpdateBody(bodyId, string(bodystr))
			if errFound(err, raw) {
				return
			}
		} else {
			err = InsertBody(systemId, bodyname, string(bodystr))
			if errFound(err, raw) {
				return
			}
		}
	}

	log.Print(bodyname, "@", systemname, " body scanned")
}

func HandleStarScanEvent(raw string, event map[string]interface{}, publisher *zmq.Socket) {
	systemname := event["StarSystem"].(string)
	bodyname := event["BodyName"].(string)

	// Fetch the current system from the database
	systemx, err := Float(event["StarPos"].([]interface{})[0])
	assertNil(err)
	systemx = fixCoord(systemx)
	systemy, err := Float(event["StarPos"].([]interface{})[1])
	assertNil(err)
	systemy = fixCoord(systemy)
	systemz, err := Float(event["StarPos"].([]interface{})[2])
	assertNil(err)
	systemz = fixCoord(systemz)

	systemdata, err := FetchSystem(systemname, systemx, systemy, systemz)
	if err != nil {
		// System doesn't exist - ignore
	} else {
		// Turn the system in to JSON
		d := json.NewDecoder(strings.NewReader(systemdata))
		d.UseNumber()
		var system map[string]interface{}
		err = d.Decode(&system)
		if errFound(err, raw) {
			return
		}

		// Now fetch the body
		systemId, err := Int(system["id"])
		if errFound(err, raw) {
			return
		}
		body, err := FetchBody(systemId, bodyname)
		var exists bool
		if err != nil {
			exists = false
			body = make(map[string]interface{})
			body["created_at"] = int32(time.Now().Unix())
		} else {
			exists = true
		}

		body["updated_at"] = int32(time.Now().Unix())

		// Age
		age, err := Int(event["Age_MY"])
		if err == nil {
			body["age"] = age
		}
		// Periapsis
		periapsis, err := Float(event["Periapsis"])
		if err == nil {
			body["arg_of_periapsis"] = periapsis
		}
		// Distance
		distance, err := Float(event["DistanceFromArrivalLS"])
		if err == nil {
			body["distance_to_arrival"] = distance
		}
		body["group_id"] = 2
		body["group_name"] = "Star"
		body["is_landable"] = 0
		if distance == 0 {
			body["is_main_star"] = true
		} else {
			body["is_main_star"] = false
		}
		body["is_rotational_period_tidally_locked"] = false
		body["name"] = bodyname
		// Orbital eccentricity
		eccentricity, err := Float(event["Eccentricity"])
		if err == nil {
			body["orbital_eccentricity"] = eccentricity
		}
		// Orbital inclination
		inclination, err := Float(event["OrbitalInclination"])
		if err == nil {
			body["orbital_inclination"] = inclination
		}
		// Orbital period
		orbitalPeriod, err := Float(event["OrbitalPeriod"])
		if err == nil {
			body["orbital_period"] = orbitalPeriod
		}
		// Rotational period
		rotationalPeriod, err := Float(event["RotationPeriod"])
		if err == nil {
			body["rotational_period"] = rotationalPeriod / 86400
		}
		// Semi-major axis
		semiMajorAxis, err := Float(event["SemiMajorAxis"])
		if err == nil {
			body["semi_major_axis"] = semiMajorAxis / 149597870700
		}
		// Stellar mass
		stellarMass, err := Float(event["StellarMass"])
		if err == nil {
			body["solar_masses"] = stellarMass
		}
		// Radius
		radius, err := Float(event["Radius"])
		if err == nil {
			body["solar_radius"] = radius / 695700000
		}
		// Stellar class
		body["spectral_class"] = event["StarType"]
		// Surface temperature
		surfaceTemperature, err := Float(event["SurfaceTemperature"])
		if err == nil {
			body["surface_temperature"] = surfaceTemperature
		}

		// Update body data
		// d := json.NewDecoder(strings.NewReader(systemdata))
		// d.UseNumber()
		// var body map[string]interface{}
		// err = d.Decode(&body)
		// if errFound(err, raw) {
		// return
		// }
		// Create or update
		bodystr, err := json.Marshal(body)
		if errFound(err, raw) {
			return
		}

		if exists {
			bodyId, err := Int(body["id"])
			if errFound(err, raw) {
				return
			}
			err = UpdateBody(bodyId, string(bodystr))
			if errFound(err, raw) {
				return
			}
		} else {
			err = InsertBody(systemId, bodyname, string(bodystr))
			if errFound(err, raw) {
				return
			}
		}
		log.Print(bodyname, "@", systemname, " star scanned")
	}
}

func HandleDockedEvent(raw string, event map[string]interface{}, publisher *zmq.Socket) {
	systemname := event["StarSystem"].(string)
	stationname := event["StationName"].(string)

	stationfaction := event["StationFaction"]
	if stationfaction == nil {
		stationfaction = ""
	}

	// For 'Docked' events a missing allegiance implies Independent
	stationallegiance := event["StationAllegiance"]
	if stationallegiance == nil {
		stationallegiance = "Faction_Independent"
	}
	stationallegiance = TranslateAllegiance(stationallegiance.(string))

	stationeconomy := event["StationEconomy"]
	if stationeconomy == nil {
		stationeconomy = ""
	}
	stationeconomy = TranslateEconomy(stationeconomy.(string))

	stationgovernment := event["StationGovernment"]
	if stationgovernment == nil {
		stationgovernment = ""
	}
	stationgovernment = TranslateGovernment(stationgovernment.(string))

	stationstate := event["FactionState"]
	if stationstate == nil {
		stationstate = ""
	}
	stationstate = TranslateState(stationstate.(string))

	// Fetch the current system from the database
	systemx, err := Float(event["StarPos"].([]interface{})[0])
	assertNil(err)
	systemx = fixCoord(systemx)
	systemy, err := Float(event["StarPos"].([]interface{})[1])
	assertNil(err)
	systemy = fixCoord(systemy)
	systemz, err := Float(event["StarPos"].([]interface{})[2])
	assertNil(err)
	systemz = fixCoord(systemz)

	systemdata, err := FetchSystem(systemname, systemx, systemy, systemz)
	if err != nil {
		// System doesn't exist - make it
	} else {
		// Turn the system in to JSON
		d := json.NewDecoder(strings.NewReader(systemdata))
		d.UseNumber()
		var system map[string]interface{}
		err = d.Decode(&system)
		if errFound(err, raw) {
			return
		}

		// Only if the event's timestamp is after the last time we updated the data
		eventTime, err := time.Parse(time.RFC3339, event["timestamp"].(string))
		if errFound(err, raw) {
			return
		}
		updateTime := IntOr(system["updated_at"], 0)
		if eventTime.Unix() > updateTime {
			systemId, err := Int(system["id"])
			if errFound(err, raw) {
				return
			}

			stationdata, err := FetchStation(systemId, stationname)
			if err != nil {
				// Station doesn't exist - create it
			} else {
				// Turn the station into JSON
				d2 := json.NewDecoder(strings.NewReader(stationdata))
				d2.UseNumber()
				var station map[string]interface{}
				err = d2.Decode(&station)
				if errFound(err, raw) {
					return
				}

				var update map[string]interface{}
				update = make(map[string]interface{})

				updaterequired := false

				dballegiance := JsonString(station["allegiance"])
				if dballegiance != stationallegiance {
					updaterequired = true
					log.Print(stationname, "@", system["name"], " station allegiance ", dballegiance, " -> ", stationallegiance)
					if dballegiance != "" {
						update["oldallegiance"] = dballegiance
						update["newallegiance"] = stationallegiance
					}
				}

				dbeconomy := JsonString(station["primary_economy"])
				if dbeconomy != stationeconomy {
					updaterequired = true
					log.Print(stationname, "@", system["name"], " station economy ", dbeconomy, " -> ", stationeconomy)
					if dbeconomy != "" {
						update["oldeconomy"] = dbeconomy
						update["neweconomy"] = stationeconomy
					}
				}

				dbgovernment := JsonString(station["government"])
				if dbgovernment != stationgovernment {
					updaterequired = true
					log.Print(stationname, "@", system["name"], " station government ", dbgovernment, " -> ", stationstate)
					if dbgovernment != "" {
						update["oldgovernment"] = dbgovernment
						update["newgovernment"] = stationgovernment
					}
				}

				dbfaction := JsonString(station["controlling_faction"])
				if dbfaction != stationfaction {
					updaterequired = true
					log.Print(stationname, "@", system["name"], " station controllling faction ", dbfaction, " -> ", stationfaction)
					if dbfaction != "" {
						update["oldfaction"] = dbfaction
						update["newfaction"] = stationfaction
					}
				}

				dbstate := JsonString(station["state"])
				if dbstate != stationstate {
					updaterequired = true
					log.Print(stationname, "@", system["name"], " station state ", dbstate, " -> ", stationstate)
					if dbstate != "" {
						update["oldstate"] = dbstate
						update["newstate"] = stationstate
					}
				}

				if updaterequired {
					// Update the database
					station["allegiance"] = stationallegiance
					station["primary_economy"] = stationeconomy
					station["government"] = stationgovernment
					station["state"] = stationstate
					station["updated_at"] = int32(time.Now().Unix())
					station["controlling_faction"] = stationfaction
					updatedStation, err := json.Marshal(station)
					if errFound(err, raw) {
						return
					}
					stationId, err := Int(station["id"])
					if errFound(err, raw) {
						return
					}
					UpdateStation(systemId, stationId, string(updatedStation))
					if errFound(err, raw) {
						return
					}

					// Send notification
					update["systemname"] = systemname
					update["stationname"] = stationname
					update["x"] = systemx
					update["y"] = systemy
					update["z"] = systemz
					updateJson, err := json.Marshal(update)
					if errFound(err, raw) {
						return
					}
					_, _ = publisher.SendMessage("eddp.delta.station", string(updateJson))
				}
			}
		}
	}
}

func HandleOutfitting2Schema(raw string, message map[string]interface{}, publisher *zmq.Socket) {
	// Obtain the system
	systemname := message["systemName"].(string)
	systemdata, err := FetchFirstSystem(systemname)
	if err == nil {
		// Turn the system in to JSON
		d := json.NewDecoder(strings.NewReader(systemdata))
		d.UseNumber()
		var system map[string]interface{}
		err = d.Decode(&system)
		if errFound(err, raw) {
			return
		}

		// Obtain the station
		stationname := message["stationName"].(string)
		systemId, err := Int(system["id"])
		if errFound(err, raw) {
			return
		}
		stationdata, err := FetchStation(systemId, stationname)
		if err == nil {
			// Turn the station in to JSON
			d := json.NewDecoder(strings.NewReader(stationdata))
			d.UseNumber()
			var station map[string]interface{}
			err = d.Decode(&station)
			if errFound(err, raw) {
				return
			}
			stationId, err := Int(station["id"])
			if errFound(err, raw) {
				return
			}

			// Only if the message's timestamp is after the last time we updated the data
			messageTime, err := time.Parse(time.RFC3339, message["timestamp"].(string))
			if errFound(err, raw) {
				return
			}
			updateTime := IntOr(station["outfitting_updated_at"], 0)
			if errFound(err, raw) {
				return
			}
			if messageTime.Unix() > updateTime {
				station["selling_modules"] = message["modules"]

				// Update timestamp
				station["outfitting_updated_at"] = int32(time.Now().Unix())
				dbstation, err := json.Marshal(station)
				err = UpdateStation(systemId, stationId, string(dbstation))
				if errFound(err, raw) {
					return
				}

				log.Print(stationname, "@", systemname, " outfitting updated")
			}
		}
	}
}

func HandleCommodity3Schema(raw string, message map[string]interface{}, publisher *zmq.Socket) {
	// Obtain the system
	systemname := message["systemName"].(string)
	systemdata, err := FetchFirstSystem(systemname)
	if err == nil {
		// Turn the system in to JSON
		d := json.NewDecoder(strings.NewReader(systemdata))
		d.UseNumber()
		var system map[string]interface{}
		err = d.Decode(&system)
		if errFound(err, raw) {
			return
		}

		// Obtain the station
		stationname := message["stationName"].(string)
		systemId, err := Int(system["id"])
		if errFound(err, raw) {
			return
		}
		stationdata, err := FetchStation(systemId, stationname)
		if err == nil {
			// Turn the station in to JSON
			d := json.NewDecoder(strings.NewReader(stationdata))
			d.UseNumber()
			var station map[string]interface{}
			err = d.Decode(&station)
			if errFound(err, raw) {
				return
			}
			stationId, err := Int(station["id"])
			if errFound(err, raw) {
				return
			}

			// Only if the message's timestamp is after the last time we updated the data
			messageTime, err := time.Parse(time.RFC3339, message["timestamp"].(string))
			if errFound(err, raw) {
				return
			}
			updateTime := IntOr(station["market_updated_at"], 0)
			if messageTime.Unix() > updateTime {

				commodities := message["commodities"].([]interface{})

				// Build updated commodities
				var dbcommodities []map[string]interface{}
				dbcommodities = make([]map[string]interface{}, len(commodities))
				// transform each
				for i := range commodities {
					var dbcommodity map[string]interface{}
					dbcommodity = make(map[string]interface{})

					commodity := commodities[i].(map[string]interface{})

					// Obtain name and ID
					name := commodity["name"]
					name = TranslateCommodity(name.(string))
					dbcommodity["name"] = name

					id, exists := commodityids.m[name.(string)]
					if !exists {
						id = -1
					}
					dbcommodity["id"] = id

					// See if it is being sold
					var stockbracket int64
					tmpstockbracket := commodity["stockBracket"]
					switch t := tmpstockbracket.(type) {
					case json.Number:
						stockbracket, err = tmpstockbracket.(json.Number).Int64()
						if errFound(err, raw) {
							return
						}
					case string:
						// This can happen if the stock bracket is "", which is rather surprisingly
						// a valid value and means "not normally but at the moment yes"
						stockbracket = 3
					default:
						log.Print("unexpected type %T\n", t)
					}
					if stockbracket > 0 {
						stock, err := Int(commodity["stock"])
						if errFound(err, raw) {
							return
						}
						if stock > 0 {
							dbcommodity["supply"] = stock
							price, err := Int(commodity["buyPrice"])
							if errFound(err, raw) {
								return
							}
							dbcommodity["buy_price"] = price
						}
					}
					// See if it is being bought
					var demandbracket int64
					tmpdemandbracket := commodity["demandBracket"]
					switch t := tmpdemandbracket.(type) {
					case json.Number:
						demandbracket, err = tmpdemandbracket.(json.Number).Int64()
						if errFound(err, raw) {
							return
						}
					case string:
						// This can happen if the demand bracket is "", which is rather surprisingly
						// a valid value and means "not normally but at the moment yes"
						demandbracket = 3
					default:
						log.Print("unexpected type %T\n", t)
					}
					if demandbracket > 0 {
						demand, err := Int(commodity["demand"])
						if errFound(err, raw) {
							return
						}
						if demand > 0 {
							dbcommodity["demand"] = demand
							price, err := Int(commodity["sellPrice"])
							if errFound(err, raw) {
								return
							}
							dbcommodity["sell_price"] = price
						}
					}
					dbcommodities[i] = dbcommodity
				}

				// Replace existing station commodities
				station["commodities"] = dbcommodities

				// Update timestamp
				station["market_updated_at"] = int32(time.Now().Unix())
				dbstation, err := json.Marshal(station)
				err = UpdateStation(systemId, stationId, string(dbstation))
				if errFound(err, raw) {
					return
				}

				log.Print(stationname, "@", systemname, " market updated")
			}
		}
	}
}

func HandleFSDJumpEvent(raw string, event map[string]interface{}, publisher *zmq.Socket) {
	systemname := event["StarSystem"].(string)

	systemsecurity := event["SystemSecurity"]
	if systemsecurity == nil {
		systemsecurity = ""
	}
	systemsecurity = TranslateSecurity(systemsecurity.(string))

	systemallegiance := event["SystemAllegiance"]
	if systemallegiance == nil {
		systemallegiance = ""
	}
	systemallegiance = TranslateAllegiance(systemallegiance.(string))

	systemeconomy := event["SystemEconomy"]
	if systemeconomy == nil {
		systemeconomy = ""
	}
	systemeconomy = TranslateEconomy(systemeconomy.(string))

	systemgovernment := event["SystemGovernment"]
	if systemgovernment == nil {
		systemgovernment = ""
	}
	systemgovernment = TranslateGovernment(systemgovernment.(string))

	systemstate := event["FactionState"]
	if systemstate == nil {
		systemstate = ""
	}
	systemstate = TranslateState(systemstate.(string))

	// Fetch the current information from the DB
	systemx, err := Float(event["StarPos"].([]interface{})[0])
	assertNil(err)
	systemx = fixCoord(systemx)
	systemy, err := Float(event["StarPos"].([]interface{})[1])
	assertNil(err)
	systemy = fixCoord(systemy)
	systemz, err := Float(event["StarPos"].([]interface{})[2])
	assertNil(err)
	systemz = fixCoord(systemz)

	systemdata, err := FetchSystem(systemname, systemx, systemy, systemz)
	if err != nil {
		// System doesn't exist - make it
		var dbsystem map[string]interface{}
		dbsystem = make(map[string]interface{})
		dbsystem["name"] = systemname
		dbsystem["x"] = systemx
		dbsystem["y"] = systemy
		dbsystem["z"] = systemz
		dbsystem["is_populated"] = false
		dbsystem["government"] = systemgovernment
		dbsystem["allegiance"] = systemallegiance
		dbsystem["state"] = systemstate
		dbsystem["security"] = systemsecurity
		dbsystem["primary_economy"] = systemeconomy
		dbsystem["updated_at"] = int32(time.Now().Unix())
		dbsystemstr, err := json.Marshal(dbsystem)
		if errFound(err, raw) {
			return
		}

		err = InsertSystem(systemname, systemx, systemy, systemz, string(dbsystemstr))
		if errFound(err, raw) {
			return
		}
	} else {
		// Turn the system in to JSON
		d := json.NewDecoder(strings.NewReader(systemdata))
		d.UseNumber()
		var system map[string]interface{}
		err = d.Decode(&system)
		if errFound(err, raw) {
			return
		}

		// Only if the event's timestamp is after the last time we updated the data
		eventTime, err := time.Parse(time.RFC3339, event["timestamp"].(string))
		if errFound(err, raw) {
			return
		}
		updateTime, err := Int(system["updated_at"])
		if errFound(err, raw) {
			return
		}
		if eventTime.Unix() > updateTime {
			// State/econonmy etc. is only valid if the system is populated
			if system["is_populated"].(bool) == true {
				var update map[string]interface{}
				update = make(map[string]interface{})

				updaterequired := false

				dbsecurity := JsonString(system["security"])
				if dbsecurity != systemsecurity {
					updaterequired = true
					log.Print(systemname, " system security ", dbsecurity, " -> ", systemsecurity)
					if dbsecurity != "" {
						update["oldsecurity"] = dbsecurity
						update["newsecurity"] = systemsecurity
					}
				}

				dballegiance := JsonString(system["allegiance"])
				if dballegiance != systemallegiance {
					updaterequired = true
					log.Print(systemname, " system allegiance ", dballegiance, " -> ", systemallegiance)
					if dballegiance != "" {
						update["oldallegiance"] = dballegiance
						update["newallegiance"] = systemallegiance
					}
				}

				dbeconomy := JsonString(system["primary_economy"])
				if dbeconomy != systemeconomy {
					updaterequired = true
					log.Print(systemname, " system economy ", dbeconomy, " -> ", systemstate)
					if dbeconomy != "" {
						update["oldeconomy"] = dbeconomy
						update["neweconomy"] = systemeconomy
					}
				}

				dbgovernment := JsonString(system["government"])
				if dbgovernment != systemgovernment {
					updaterequired = true
					log.Print(systemname, " system government ", dbgovernment, " -> ", systemstate)
					if dbgovernment != "" {
						update["oldgovernment"] = dbgovernment
						update["newgovernment"] = systemgovernment
					}
				}

				dbstate := JsonString(system["state"])
				if dbstate != systemstate {
					updaterequired = true
					log.Print(systemname, " system state ", dbstate, " -> ", systemstate)
					if dbstate != "" {
						update["oldstate"] = dbstate
						update["newstate"] = systemstate
					}
				}

				if updaterequired {
					// Update the database
					system["security"] = systemsecurity
					system["allegiance"] = systemallegiance
					system["primary_economy"] = systemeconomy
					system["government"] = systemgovernment
					system["state"] = systemstate
					system["updated_at"] = int32(time.Now().Unix())
					updatedSystem, err := json.Marshal(system)
					if errFound(err, raw) {
						return
					}
					systemId, err := Int(system["id"])
					if errFound(err, raw) {
						return
					}
					UpdateSystem(systemId, string(updatedSystem))
					if errFound(err, raw) {
						return
					}

					// Send notification
					update["systemname"] = systemname
					update["x"] = systemx
					update["y"] = systemy
					update["z"] = systemz
					updateJson, err := json.Marshal(update)
					if errFound(err, raw) {
						return
					}
					_, _ = publisher.SendMessage("eddp.delta.system", string(updateJson))
				}
			}
		}
	}
}

func JsonString(obj interface{}) string {
	if obj != nil {
		return obj.(string)
	}
	return ""
}

func TranslateSecurity(security string) string {
	if security == "" {
		return "None"
	}
	if translated, present := securities.m[strings.Replace(strings.Replace(strings.ToLower(security), "$", "", -1), ";", "", -1)]; present {
		return translated
	}
	return security
}

func TranslateAllegiance(allegiance string) string {
	if allegiance == "" {
		return "None"
	}
	if translated, present := allegiances.m[strings.Replace(strings.Replace(strings.ToLower(allegiance), "$", "", -1), ";", "", -1)]; present {
		return translated
	}
	return allegiance
}

func TranslateEconomy(economy string) string {
	if economy == "" {
		return "None"
	}
	if translated, present := economies.m[strings.Replace(strings.Replace(strings.ToLower(economy), "$", "", -1), ";", "", -1)]; present {
		return translated
	}
	return economy
}

func TranslateGovernment(government string) string {
	if government == "" {
		return "None"
	}
	if translated, present := governments.m[strings.Replace(strings.Replace(strings.ToLower(government), "$", "", -1), ";", "", -1)]; present {
		return translated
	}
	return government
}

func TranslateState(state string) string {
	if state == "" {
		return "None"
	}
	if translated, present := states.m[strings.Replace(strings.Replace(strings.ToLower(state), "$", "", -1), ";", "", -1)]; present {
		return translated
	}
	return state
}

func TranslateVolcanism(volcanism string) string {
	if volcanism == "" {
		return "None"
	}
	if translated, present := volcanisms.m[strings.Replace(strings.Replace(strings.ToLower(volcanism), "$", "", -1), ";", "", -1)]; present {
		return translated
	}
	return volcanism
}

func TranslateMaterial(material string) string {
	if material == "" {
		return "None"
	}
	if translated, present := materials.m[strings.Replace(strings.Replace(strings.Replace(strings.ToLower(material), "$", "", -1), ";", "", -1), " ", "", -1)]; present {
		return translated
	}
	return material
}

func TranslateCommodity(commodity string) string {
	if commodity == "" {
		return "None"
	}
	if translated, present := commodities.m[strings.Replace(strings.Replace(strings.Replace(strings.ToLower(commodity), "$", "", -1), ";", "", -1), " ", "", -1)]; present {
		return translated
	}
	return commodity
}

func FetchFirstSystem(system string) (string, error) {
	var data string
	err := eddpDb.QueryRow("SELECT data FROM systems WHERE name = ? LIMIT 1", system).Scan(&data)
	if err != nil {
		return "", errors.New("No such system")
	}

	return data, nil
}

func FetchBody(systemId int64, body string) (map[string]interface{}, error) {
	var data string
	err := eddpDb.QueryRow("SELECT data FROM bodies WHERE CAST(system_id AS INT) = ? AND name = ?", systemId, body).Scan(&data)
	if err != nil {
		bodyJson := make(map[string]interface{})
		return bodyJson, errors.New("No such body")
	}
	d := json.NewDecoder(strings.NewReader(data))
	d.UseNumber()
	var bodyJson map[string]interface{}
	err = d.Decode(&bodyJson)
	if errFound(err, body) {
		return bodyJson, errors.New("Invalid body JSON")
	}

	return bodyJson, nil
}

func FetchSystem(system string, x float64, y float64, z float64) (string, error) {
	var data string
	err := eddpDb.QueryRow("SELECT data FROM systems WHERE name = ? and CAST(x AS FLOAT) = ? and CAST(y AS FLOAT) = ? and CAST(z as FLOAT) = ?", system, x, y, z).Scan(&data)
	if err != nil {
		return "", errors.New("No such system")
	}

	return data, nil
}

func FetchStation(systemId int64, station string) (string, error) {
	var dataId int
	var data string
	err := eddpDb.QueryRow("SELECT id, data FROM stations WHERE system_id = ? AND name = ?", systemId, station).Scan(&dataId, &data)
	if err != nil {
		return "", errors.New("No such station")
	}

	return data, nil
}

func InsertSystem(name string, x float64, y float64, z float64, system string) error {
	// Obtain the next ID
	var nextId int
	err := eddpDb.QueryRow("SELECT max(id) + 1 FROM systems").Scan(&nextId)
	if err != nil {
		return err
	}

	// Splice the ID in to the system information
	system = system[:len(system)-1]
	system = system + ",\"id\":"
	system = system + strconv.Itoa(nextId)
	system = system + "}"

	log.Print(name, " created (", nextId, ")")

	// Retry up to 5 times...
	retrying := 5
	for retrying > 0 {
		_, err = eddpDb.Exec("INSERT INTO systems(id, name, x, y, z, data) VALUES(?, ?, ?, ?, ?, ?)", nextId, name, x, y, z, system)
		if err != nil {
			// Failed to do it this time, wait for a second to retry
			time.Sleep(1000 * time.Millisecond)
			retrying--
		} else {
			// Success
			break
		}
	}
	return err
}

func UpdateSystem(systemId int64, system string) error {
	_, err := eddpDb.Exec("UPDATE systems SET data = ? WHERE id = ?", system, systemId)
	return err
}

func InsertBody(systemId int64, name string, body string) error {
	// Obtain the next ID
	var nextId int
	err := eddpDb.QueryRow("SELECT max(id) + 1 FROM bodies").Scan(&nextId)
	if err != nil {
		return err
	}

	// Splice the ID in to the body information
	body = body[:len(body)-1]
	body = body + ",\"id\":"
	body = body + strconv.Itoa(nextId)
	body = body + "}"

	log.Print(name, " created (", nextId, ")")

	// Retry up to 5 times...
	retrying := 5
	for retrying > 0 {
		_, err = eddpDb.Exec("INSERT INTO bodies(system_id, id, name, data) VALUES(?, ?, ?, ?)", systemId, nextId, name, body)
		if err != nil {
			// Failed to do it this time, wait for a second to retry
			time.Sleep(1000 * time.Millisecond)
			retrying--
		} else {
			// Success
			break
		}
	}
	return err
}

func UpdateBody(bodyId int64, body string) error {
	_, err := eddpDb.Exec("UPDATE bodies SET data = ? WHERE id = ?", body, bodyId)
	return err
}

func UpdateStation(systemId int64, stationId int64, station string) error {
	_, err := eddpDb.Exec("UPDATE stations SET data = ? WHERE system_id = ? AND id = ?", station, systemId, stationId)
	return err
}

func fixCoord(a float64) float64 {
	if a < 0 {
		return float64(int(math.Ceil(a*32-0.5))) / 32
	}
	return float64(int(math.Floor(a*32+0.5))) / 32
}

func Float(val interface{}) (float64, error) {
	switch val.(type) {
	case nil:
		return 0, errors.New("Missing value")
	case json.Number:
		return val.(json.Number).Float64()
	default:
		return 0, errors.New("Invalid value type")
	}
}

func Int(val interface{}) (int64, error) {
	switch val.(type) {
	case nil:
		return 0, errors.New("Missing value")
	case json.Number:
		return val.(json.Number).Int64()
	default:
		return 0, errors.New("Invalid value type")
	}
}

func IntOr(val interface{}, defval int64) int64 {
	switch val.(type) {
	case nil:
		return defval
	case json.Number:
		ret, err := val.(json.Number).Int64()
		if err != nil {
			return defval
		}
		return ret
	default:
		return defval
	}
}
