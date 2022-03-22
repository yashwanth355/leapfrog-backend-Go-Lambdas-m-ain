package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"bytes"
	"net/smtp"
	"text/template"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"
)

const (
	host     = "ccl-psql-dev.cclxlbtddgmn.ap-south-1.rds.amazonaws.com"
	port     = 5432
	user     = "postgres"
	password = "Ccl_RDS_DB#2022"
	dbname   = "ccldevdb"
	//email-SMTP
	from_email = "itsupport@continental.coffee"
	smtp_pass  = "is@98765"
	// smtp server configuration.
	smtpHost = "smtp.gmail.com"
	smtpPort = "587"
)
const poTemp = `<!DOCTYPE html>
	    <html>
		<head>
			<img src="https://s3.ap-south-1.amazonaws.com/beta-a2z.cclproducts.com/static/media/CCLEmailTemplate.png">
		</head>
		<body>
			<h3>Hello {{.EName}},</h3>
			<p>{{.EMessage}}</p>
			<p>{{.EDept}}</p>
		</body>
	</html>`

type Email struct {
	ToName      string `json:"name"`
	Subject     string `json:"subject"`
	Message     string `json:"message"`
	ToUserEmail string `json:"to_email"`
}

type PurchaseOrderDetails struct {
	Create          bool   `json:"po_create"`
	Update          bool   `json:"po_update"`
	View            bool   `json:"po_view"`
	Status          string `json:"status"`
	CreatedUserID   string `json:"createduserid"`
	CreatedUserName string `json:"createdusername"`
	UserEmail       string `json:"user_email"`
	//Contract Information
	Contract string `json:"contract"`
	// CIdsNo			        int    `json:"contract_idsno"`
	// CNO           			string `json:"contract_no"`
	// CNOsno        			int    `json:"contract_cnosno"`

	//PO Info Section::
	POTypeID        string `json:"po_type_id"`
	PoId            string `json:"poid"`
	PoIdsNo         int    `json:"poidsno"`
	PoNO            string `json:"po_no"`
	PoNOsno         int    `json:"po_nosno"`
	PoDate          string `json:"po_date"`
	POCategory      string `json:"po_category"`
	POSubCategory   string `json:"po_sub_category"`
	SupplierTypeID  string `json:"supplier_type_id"`
	SupplierCountry string `json:"supplier_country"`
	//---------Currency & Advance Information//------------------
	CurrencyID   string `json:"currency_id"`
	CurrencyName string `json:"currency_name"`
	CurrencyCode string `json:"currency_code"`

	//Supplier/Vendor Information
	SupplierName    string `json:"supplier_name"`
	SupplierID      string `json:"supplier_id"`
	SupplierType    string `json:"supplier_type"`
	SupplierEmail   string `json:"supplier_email"`
	SupplierAddress string `json:"supplier_address"`

	//Vendor      			string `json:"supplier_id"`
	// VendorType  			string `json:"vendor_type"`
	QuotNo    string `json:"quot_no"`
	QuotDate  string `json:"quot_date"`
	QuotPrice string `json:"quot_price"`

	LastPoIdsno int `json:"last_poidsno"`
	//currency & incoterms
	IncoTermsID string `json:"incotermsid"`
	IncoTerms   string `json:"incoterms"`
	Origin      string `json:"origin"`
	PortOfLoad  string `json:"ports"`
	// TransportMode		 	string `json:"mode_of_transport"`
	Insurance        string `json:"insurance"`
	DPortId          string `json:"destination_port_id"`
	DPortName        string `json:"destination_port_name"`
	Forwarding       string `json:"forwarding"`
	NoOfContainers   string `json:"no_of_containers"`
	ContainerType    string `json:"container_type"`
	PaymentTerms     string `json:"payment_terms"`
	Comments         string `json:"comments"`
	PaymentTermsDays string `json:"payment_terms_days"` //int to string
	//Billing & Delivery Info
	POBillTypeID   string `json:"billing_at_id"`
	POBillTypeName string `json:"billing_at_name"`
	POBillAddress  string `json:"billing_at_address"`
	PODelTypeID    string `json:"delivery_at_id"`
	PODelTypeName  string `json:"delivery_at_name"`
	PODelAddress   string `json:"delivery_at_address"`

	//Green Coffee Info Section-Done--------------------------

	ItemID        string  `json:"item_id"`
	ItemName      string  `json:"item_name"`
	TotalQuantity string  `json:"total_quantity"`
	MT_Quantity   float64 `json:"quantity_mt"`
	TotalBalQuan  string  `json:"total_Balance_quantity"`

	//-----------GC- Composition Info
	Density       string `json:"density"`
	Moisture      string `json:"moisture"`
	Browns        string `json:"browns"`
	Blacks        string `json:"blacks"`
	BrokenBits    string `json:"brokenbits"`
	InsectedBeans string `json:"insectedbeans"`
	Bleached      string `json:"bleached"`
	Husk          string `json:"husk"`
	Sticks        string `json:"sticks"`
	Stones        string `json:"stones"`
	BeansRetained string `json:"beansretained"`

	//Price Information-Done------------------------------

	PurchaseType       string    `json:"purchase_type"`
	TerminalMonth      time.Time `json:"terminal_month"`
	TerminalPrice      string    `json:"terminal_price"`
	BookedTerminalRate string    `json:"booked_terminal_rate"`
	BookedDifferential string    `json:"booked_differential"`
	FixedTerminalRate  string    `json:"fixed_terminal_rate"`
	FixedDifferential  string    `json:"fixed_differential"`
	PurchasePrice      string    `json:"purchase_price"`
	MarketPrice        string    `json:"market_price"`
	POMargin           string    `json:"po_margin"`
	TotalPrice         string    `json:"totalPrice"`

	//domestic section
	PurchasePriceInr string `json:"purchasePriceInr"`
	MarketPriceInr   string `json:"marketPriceInr"`
	// FinalPriceInr    string `json:"finalPriceInr"`
	GrossPrice     string `json:"grossPrice"`
	DTerminalPrice string `json:"terminalPrice"`
	Advance        string `json:"advance"`      //changed
	AdvanceType    string `json:"advance_type"` //changed
	PoQty          string `json:"po_qty"`
	Price          string `json:"price"`
	LastTaxIdsno   int    `json:"last_taxidsno"`
	TaxIdsno       int    `json:"taxidsno"`
	TaxId          string `json:"tax_id"`
	Tds            string `json:"tds"`
	//Status -PO
	ApprovalStatus bool `json:"approval_status"`

	//GC Information-Dispatch Section

	DispatchType  string `json:"dispatch_type"`
	DispatchCount string `json:"dispatch_count"`

	LastDetIDSNo int    `json:"last_det_ids_no"`
	DetIDSNo     int    `json:"det_ids_no"`
	DetID        string `json:"det_id_no"`
	// DispatchID			string `json:"dispatch_id"`
	ItemDispatchDetails []ItemDispatch `json:"item_dispatch"`

	// Domestic Tax Info
	SGST string `json:"sgst"`
	CGST string `json:"cgst"`
	IGST string `json:"igst"`
	//Other Information
	TaxDuties        string `json:"taxes_duties"`
	ModeOfTransport  string `json:"mode_of_transport"`
	TransitInsurance string `json:"transit_insurance"`
	PackForward      string `json:"packing_forwarding"`
	OtherCharges     string `json:"otherCharges"`
	// DomesticCharges     DomesticCharges `json:"domestic_otherCharges"`
	Rate            string         `json:"rate"`
	AuditLogDetails []AuditLogGCPO `json:"audit_log_gc_po"`

	//Extra fields added
	FixationDate string `json:"fixation_date"`
	NoofBags     string `json:"no_of_bags"`
	NetWeight    string `json:"net_weight"`
	//Other Charges--Domestic
	DPackForward  string `json:"packing_forward_charges"`
	DInstallation string `json:"installation_charges"`
	DFreight      string `json:"freight_charges"`
	DHandling     string `json:"handling_charges"`
	DMisc         string `json:"misc_charges"`
	DHamali       string `json:"hamali_charges"`
	DMandiFee     string `json:"mandifee_charges"`
	DFullTax      string `json:"fulltax_charges"`
	DInsurance    string `json:"insurance_charges"`

	//Documents Section
	DocumentsSection []DocumentsUpload `json:"documentsection"`
}
type ItemDispatch struct {
	DispatchID       string `json:"dispatch_id"`
	DispatchQuantity string `json:"dispatch_quantity"`
	DispatchDate     string `json:"dispatch_date"`
	DSNo             string `json:"number"`
	DDate            string `json:"date"`
}
type OtherChargesTaxes struct {
	TaxId   string `json:"tax_id"`
	TaxName string `json:"tax_name"`
	TaxPercentage int `json:"tax_percentage"`

	MiscId      string `json:"misc_id"`
	ChargesName string `json:"misc_charges_name"`
	MiscRate 	string `json:"misc_charge_rate"`
	TotalTaxesRate  string `json:"total_charges"`
}



func NewNullString(s string) sql.NullString {
	if len(s) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}
func NewNullTime(t time.Time) sql.NullTime {
	if t.IsZero() {
		return sql.NullTime{}
	}
	return sql.NullTime{
		Time:  t,
		Valid: true,
	}
}
func insertGCPODetails(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	headers := map[string]string{"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Origin, X-Requested-With, Content-Type, Accept"}
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	var po PurchaseOrderDetails
	var audit AuditLogGCPO
	var email Email
	// var dc DomesticCharges

	err := json.Unmarshal([]byte(request.Body), &po)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Println(err)
		return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
	}
	defer db.Close()

	// check db
	err = db.Ping()

	if err != nil {
		log.Println(err)
		return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
	}

	fmt.Println("Connected!")
	var rows *sql.Rows

	if po.Create {
		log.Println("Entered Create Module")

		log.Println("Created user id is: ", po.CreatedUserID)
		// Find created user username
		sqlStatementCUser1 := `SELECT username,emailid 
							FROM dbo.users_master_newpg 
							where userid=$1`
		rows, err = db.Query(sqlStatementCUser1, po.CreatedUserID)
		for rows.Next() {
			err = rows.Scan(&po.CreatedUserName, &po.UserEmail)
		}

		//Find latest poid
		sqlStatementPOF1 := `SELECT poidsno 
							FROM dbo.pur_gc_po_con_master_newpg 
							where poidsno is not null
							ORDER BY poidsno DESC 
							LIMIT 1`
		rows, err = db.Query(sqlStatementPOF1)

		// var po InputAdditionalDetails
		for rows.Next() {
			err = rows.Scan(&po.LastPoIdsno)
		}
		//Generating PO NOs----------------
		po.PoIdsNo = po.LastPoIdsno + 1
		po.PoId = "POID-" + strconv.Itoa(po.PoIdsNo)
		po.PoNOsno = po.PoIdsNo
		//Generating Contract Nos---------------
		// po.CIdsNo = po.LastPoIdsno + 1
		// po.CNOsno = po.CIdsNo
		// po.CId = "CID-" + strconv.Itoa(po.CIdsNo)
		// po.CNO = "CCL/" + strconv.Itoa(po.CNOsno) + "/" + "2021-2022"
		floatquan, _ := strconv.ParseFloat(po.TotalQuantity, 64)
		log.Println("Formattted total quantity: ", floatquan)
		po.MT_Quantity = floatquan / 1000
		log.Println("quantity in MT: ", po.MT_Quantity)
		po.TotalBalQuan = po.TotalQuantity //total balance quantity in kgs
		audit.CreatedUserid = po.CreatedUserID
		audit.CreatedDate = po.PoDate
		audit.Description = "PO Created"

		if po.SupplierType == "1001" && {
			log.Println("Entered Import Module")
			if po.CurrencyID == "" {
				po.CurrencyID = "HO-102"
			}
			log.Println("Selected supplier type Import Code:", po.SupplierType)
			
			po.PoNO = "S4002-" + strconv.Itoa(po.PoIdsNo)
			po.POSubCategory = "Import"
			//-----------------END OF IMPORT MODULE---------------------------------------
		} else if po.SupplierType == "1002" {
			log.Println("Selected supplier type Domestic Code:", po.SupplierType)
			if po.CurrencyID == "" {
				po.CurrencyID = "HO-101"
			}			
			po.POSubCategory = "Domestic"

			po.PoNO = "CCL/" + "/" + po.POCategory + "/" + strconv.Itoa(po.PoIdsNo) + "/" + strconv.Itoa(time.Now().Year()) + "-" + strconv.Itoa(time.Now().Year()+1)
		}
		sqlStatementPOHeader := `INSERT INTO dbo.pur_gc_po_con_master_newpg(
			poid, poidsno, pono, ponosno, podate, pocat, posubcat, vendorid, dispatchterms, origin,
			poloading, insurance, destination, forwarding, currencyid, nocontainers, payment_terms, remarks, payment_terms_days, billing_at_id,
			delivery_at_id, cid, reqapproval, createdby, createdon,approvalstatus, status, quote_no, quote_date,quote_price
			purchase_type, container_type, noofbags,accpay_status, qc_status, tds, transport_mode,advancetype,advance)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,$15,$16,$17,$18, $19, $20, $21, $22,$23, $24, $25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39);`
		_, err := db.Query(sqlStatementPOHeader,
			po.PoId,
			po.PoIdsNo,
			po.PoNO,
			po.PoNOsno,
			po.PoDate,
			po.POCategory,
			po.POSubCategory,
			po.SupplierID,
			po.IncoTerms,
			po.Origin,
			po.PortOfLoad,
			po.Insurance,
			po.DPortId,
			po.Forwarding,
			po.CurrencyID,
			NewNullString(po.NoOfContainers),
			po.ContainerType,
			po.PaymentTerms,
			po.Comments,
			po.POBillTypeID,
			po.PODelTypeID,
			po.Contract,
			false,
			po.CreatedUserID,
			po.PoDate,
			false,
			1,
			NewNullString(po.QuotNo),
			NewNullString(po.QuotDate),
			NewNullString(po.QuotPrice),
			po.PurchaseType,
			po.ContainerType,
			po.NoofBags,
			"Pending", "Backlog",
			po.Tds,
			po.ModeOfTransport,
			po.AdvanceType,
			po.Advance)
			log.Println("Insert into PO Table Executed")

			if err != nil {
				log.Println(err.Error())
				return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
			}
		sqlStatementPOLine:=`INSERT INTO dbo.pur_gc_po_details_newpg(
			podetid, podetidsno, poid, itemid, rate, cid, terminal_month,booked_term_rate,booked_differential,fixed_term_rate,
			fixed_differential, market_price, po_margin, total_price, terminal_price, createdby,createdon,purchase_type, gross_price, fixationdate,
			netweight, quantity_mt, accpay_status,qc_status,balance_quantity,purchase_price,total_quantity)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,$15,$16,$17,$18, $19, $20, $21, $22,$23, $24, $25,$26,$27);`
			_, err := db.Query(sqlStatementPOLine,
				podetid, podetidsno,
				po.PoId,
				po.ItemID,
				NewNullString(po.Rate),
				NewNullString(po.Contract),
				NewNullTime(po.TerminalMonth),
				NewNullString(po.BookedTerminalRate),
				NewNullString(po.BookedDifferential),
				NewNullString(po.FixedTerminalRate),
				NewNullString(po.FixedDifferential),
				NewNullString(po.MarketPrice),
				NewNullString(po.POMargin),
				NewNullString(po.TotalPrice),
				NewNullString(po.DTerminalPrice),
				po.CreatedUserID,
				po.PoDate,
				po.PurchaseType,
				po.GrossPrice,
				po.FixationDate,
				po.NetWeight,
				po.MT_Quantity,
				"Pending", "Backlog",
				po.TotalBalQuan,
				NewNullString(po.PurchasePriceInr),
				po.TotalQuantity)
			log.Println("Insert into PO Table Executed")

			if err != nil {
				log.Println(err.Error())
				return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
			}


		

	}

	return events.APIGatewayProxyResponse{200, headers, nil, string("success"), false}, nil
}

