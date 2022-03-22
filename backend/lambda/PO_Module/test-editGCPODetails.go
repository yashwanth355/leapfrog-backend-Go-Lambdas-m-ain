//Deployed with Pwd changes-Aug22
//Deployed updated -Sep1
//Deployed updated -Sep6
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

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
)

type PurchaseOrderDetails struct {
	Create          bool   `json:"po_create"`
	Update          bool   `json:"po_update"`
	View            bool   `json:"po_view"`
	DispatchUpdate  bool   `json:"dispatch_items_update"`
	CreatedUserID   string `json:"createduserid"`
	GCCreatedUserID string `json:"gccreateduserid"`
	// CreatedUserName 		string `json:"createdusername"`
	//Contract Information
	Contract string `json:"contract"`

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

	DPortId          string `json:"destination_port_id"`
	DPortName        string `json:"destination_port_name"`
	Forwarding       string `json:"forwarding"`
	NoOfContainers   string `json:"no_of_containers"`
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
	NetWeight    string `json:"net_weight"`
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
	//new fields
	NoofBags     string `json:"no_of_bags"`
	NetWt         string `json:"net_weight"`
	ContainerType string `json:"container_type"`

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

	ApprovalStatus bool `json:"approval_status"`

	//GC Information-Dispatch Section

	DispatchType  string `json:"dispatch_type"`
	DispatchCount string `json:"dispatch_count"`

	LastDetIDSNo int    `json:"last_det_ids_no"`
	DetIDSNo     int    `json:"det_ids_no"`
	DetID        string `json:"det_id_no"`
	// DispatchID			string `json:"dispatch_id"`
	ItemDispatchDetails []ItemDispatch `json:"item_dispatch"`

	//Domestic Price
	// TotalPrice			 string `json:"final_price"`

	//domestic section
	PurchasePriceInr string `json:"purchasePriceInr"`
	MarketPriceInr   string `json:"marketPriceInr"`
	// FinalPriceInr		 string `json:"finalPriceInr"`
	GrossPrice     string `json:"grossPrice"`
	DTerminalPrice string `json:"terminalPrice"`
	Advance        string `json:"advance"`      //changed
	AdvanceType    string `json:"advance_type"` //changed
	PoQty          string `json:"po_qty"`
	Price          string `json:"price"`
	Tds            string `json:"tds"`

	// Domestic Tax Info

	//Other Information
	TaxDuties        string `json:"taxes_duties"`
	ModeOfTransport  string `json:"mode_of_transport"`
	TransitInsurance string `json:"transit_insurance"`

	Rate            string         `json:"rate"`
	AuditLogDetails []AuditLogGCPO `json:"audit_log_gc_po"`
	//Other charges+tax info
	TaxId        string `json:"tax_id"`
	SGST         string `json:"sgst"`
	CGST         string `json:"cgst"`
	IGST         string `json:"igst"`
	PackForward  string `json:"packing_forwarding"`
	OtherCharges string `json:"otherCharges"`
	
	Insurance string `json:"insurance"`
	
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
	FixationDate string `json:"fixation_date"`
	//Documents Section
	DocumentsSection []DocumentsUpload `json:"documentsection"`
	MiscChargesDetails []MiscCharges `json:"taxes_misc_charges"`
}
type ItemDispatch struct {
	DispatchID       string `json:"dispatch_id"`
	DispatchQuantity string `json:"dispatch_quantity"`
	DispatchDate     string `json:"dispatch_date"`
	DSNo             string `json:"number"`
	DDate            string `json:"date"`
}
type AuditLogGCPO struct {
	CreatedDate    string `json:"createddate"`
	CreatedUserid  string `json:"createduserid"`
	ModifiedDate   string `json:"modifieddate"`
	ModifiedUserid string `json:"modifieduserid"`
	Description    string `json:"description"`
}

type DocumentsUpload struct {
	DocId    string `json:"docid"`
	DocKind  string `json:"doc_kind"`
	Required bool   `json:"required"`
}
type MiscCharges struct {
	TaxId   string `json:"taxid"`
	TaxName string `json:"tax_name"`
	TaxPercentage string `json:"tax_percentage"`
	MiscId      string `json:"misc_id"`
	ChargesName string `json:"misc_charge_name"`
	MiscRate 	string `json:"misc_charge_rate"`
	TotalTaxesRate  string `json:"total_tax_rate"`
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
var PsqlInfo = fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
var rows *sql.Rows
var po PurchaseOrderDetails
func editGCPODetails(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	headers := map[string]string{"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Origin, X-Requested-With, Content-Type, Accept"}
	var audit AuditLogGCPO
	err := json.Unmarshal([]byte(request.Body), &po)
	db, err := sql.Open("postgres", PsqlInfo)
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
	if po.Update {
		log.Println("Entered Edit Module")
		log.Println("Created user id is: ", po.CreatedUserID)		
		floatquan, _ := strconv.ParseFloat(po.TotalQuantity, 64)
		log.Println("Formattted total quantity: ", floatquan)
		po.MT_Quantity = floatquan / 1000
		log.Println("quantity in MT: ", po.MT_Quantity)
		po.TotalBalQuan = po.TotalQuantity
		if po.SupplierType == "1001" {
			log.Println("Entered Import Module")
			if po.CurrencyID == "" {
				po.CurrencyID = "HO-102"
			}
			log.Println("Selected supplier type Import Code:", po.SupplierType)			
			po.PoNO = "S4002-" + strconv.Itoa(po.PoIdsNo)
			po.POSubCategory = "Import"		
			
		} else if po.SupplierType == "1002" {
			log.Println("Selected supplier type Domestic Code:", po.SupplierType)
			if po.CurrencyID == "" {
				po.CurrencyID = "HO-101"
			}
			//-----------DOMESTIC INFO INSERT-----------------------
			po.POSubCategory = "Domestic"
			po.PurchasePrice =po.PurchasePriceInr
			po.PoNO = "CCL/" + "/" + po.POCategory + "/" + strconv.Itoa(po.PoIdsNo) + "/" + strconv.Itoa(time.Now().Year()) + "-" + strconv.Itoa(time.Now().Year()+1)
					
			if po.IGST !=""{				
				insertDomesticTax("FAC-38","IGST",po.ItemID, po.PoNO, po.IGST, po.PoId)
			} else if po.CGST !="" {
				insertDomesticTax("FAC-37","CGST",po.ItemID, po.PoNO, po.CGST, po.PoId)						
			}else if po.SGST!="" {				
				insertDomesticTax("FAC-36","SGST",po.ItemID, po.PoNO, po.SGST, po.PoId)
			}			
		}

		log.Println(po.ModeOfTransport,po.TaxDuties,po.PackForward,po.TransitInsurance)
		sqlUpdatePOHeader := `UPDATE dbo.pur_gc_po_con_master_newpg
							SET vendorid=$1,
							dispatchterms=$2,
							origin=$3,
							poloading=$4,
							insurance=$5,
							destination=$6,
							forwarding=$7							
							where pono=$8`
		log.Println("Update PO Header")
			_, err1 := db.Query(sqlUpdatePOHeader,
				po.SupplierID,
				po.IncoTerms,
				po.Origin,
				po.PortOfLoad,
				po.Insurance,
				po.DPortId,
				po.Forwarding,			
				po.PoNO)
			log.Println("Update into PO Table Executed")
			if err1 != nil {
				log.Println(err1.Error())
				return events.APIGatewayProxyResponse{500, headers, nil, err1.Error(), false}, nil
			}
			

		// 	//-------Delete dispatches for the PO--------------//
		result:=deleteRecords("dbo.pur_gc_po_dispatch_master_newpg",po.PoNO)		
		if result != "success" {
			log.Println("unable to delete GC Multi dispatch Details")
			return events.APIGatewayProxyResponse{500, headers, nil, result, false}, nil
					
		}
		//Update PO Line Items
		
		log.Println("Dispatches for the PO have been deleted", po.PoNO)
		//----------------Create Fresh Dispatches-------------//
		log.Println("Entered PO Create Module")
		
		log.Println("Last DETIDSNO from table:", po.LastDetIDSNo)
		// po.DetIDSNo = po.LastDetIDSNo + 1
		po.DetIDSNo=findLatestSerial("detidsno","dbo.pur_gc_po_dispatch_master_newpg","detidsno","detidsno")
		
		log.Println("New DETIDSNO from table:", po.DetIDSNo)
		po.DetID = "GCDIS-" + strconv.Itoa(po.DetIDSNo)
		log.Println("New DETID from table:", po.DetID)
		log.Println("Dispatch Type Selected:", po.DispatchType)

		result=deleteRecords("dbo.pur_gc_po_master_documents",po.PoNO)		
		if result != "success" {
			log.Println("unable to delete GC master documents")
			return events.APIGatewayProxyResponse{500, headers, nil, result, false}, nil
					
		}
		log.Println("Po documents section is successfully deleted", po.PoId)

		for _, dis := range po.ItemDispatchDetails {
			log.Println("Loop Entered")
			if po.DispatchType == "Single" {
				po.DispatchCount = "1"
				po.DispatchType = "Single"
			} else {
				po.DispatchType = "Multiple"
			}
			log.Println("Values of dispatch details are ", dis)
			sqlStatementMD1 := `insert into dbo.pur_gc_po_dispatch_master_newpg(
						pono,
						detid,
						detidsno,
						itemid,
						quantity,
						dispatch_date,
						dispatch_count,
						dispatch_type,
						createdon,
						createdby) values($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
			_, errMD1 := db.Query(sqlStatementMD1,
				po.PoNO,
				po.DetID,
				po.DetIDSNo,
				po.ItemID,
				dis.DispatchQuantity,
				dis.DispatchDate,
				po.DispatchCount,
				po.DispatchType,
				po.PoDate,
				NewNullString(po.CreatedUserID))
			log.Println("Row Inserted Successfully")
			if po.DocumentsSection != nil && len(po.DocumentsSection) > 0 {
				for i, document := range po.DocumentsSection {
					log.Println("document in loop", i, document)

					sqlStatement := `select docidsno from dbo.pur_gc_po_master_documents order by docidsno DESC LIMIT 1`
					rows1, err1 := db.Query(sqlStatement)

					if err1 != nil {
						log.Println("Unable to get last updated id")
						return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
					}

					var lastDoc int
					for rows1.Next() {
						err = rows1.Scan(&lastDoc)
					}

					docIdsno := lastDoc + 1
					docId := "FAC-" + strconv.Itoa(docIdsno)
					sqlStatement1 := `INSERT INTO dbo.pur_gc_po_master_documents (docid, docidsno, poid, dockind, required, dispatchid) VALUES ($1, $2, $3, $4, $5, $6)`
					rows, err = db.Query(sqlStatement1, docId, docIdsno, po.PoId, document.DocKind, document.Required, po.DetID)
				}
			}
			if po.DispatchType == "Multiple" {
				po.DetIDSNo = po.DetIDSNo + 1
				po.DetID = "GCDIS-" + strconv.Itoa(po.DetIDSNo)
			}

			if errMD1 != nil {
				log.Println("unable to insert GC Multi dispatch Details", errMD1)
			}
			log.Println("Inserted details are :", po.ItemDispatchDetails)
		}
		//delete & insert other charges from
		//Update PO Line Items
		
		log.Println("Dispatches for the PO have been deleted", po.PoNO)
		//----------------Create Fresh Dispatches-------------//
		log.Println("Entered PO Create Module")
		
		log.Println("Last DETIDSNO from table:", po.LastDetIDSNo)
		// po.DetIDSNo = po.LastDetIDSNo + 1
		po.DetIDSNo=findLatestSerial("detidsno","dbo.pur_gc_po_dispatch_master_newpg","detidsno","detidsno")
		
		log.Println("New DETIDSNO from table:", po.DetIDSNo)
		po.DetID = "GCDIS-" + strconv.Itoa(po.DetIDSNo)
		log.Println("New DETID from table:", po.DetID)
		log.Println("Dispatch Type Selected:", po.DispatchType)

		result=deleteRecords("dbo.pur_gc_po_details_taxes_newpg",po.PoNO)		
		if result != "success" {
			log.Println("unable to delete taxes for PO")
			return events.APIGatewayProxyResponse{500, headers, nil, result, false}, nil
					
		}
		log.Println("Po Taxes is successfully deleted", po.PoNO)
		result=deleteRecords("dbo.pur_gc_po_misc_details_newpg",po.PoNO)		
		if result != "success" {
			log.Println("unable to delete GC master documents", result)
			return events.APIGatewayProxyResponse{500, headers, nil, result, false}, nil
					
		}
		log.Println("Po Misc Taxes section is successfully deleted", po.PoId)

		miscids:=findLatestSerial("podetidsno","dbo.pur_gc_po_misc_details_newpg","podetidsno","podetidsno")
		log.Println("Finding tax serial")
		taxids :=findLatestSerial("idsno","dbo.pur_gc_po_details_taxes_newpg","idsno","idsno")
			
		
		for _, tax := range po.MiscChargesDetails {
			log.Println("Entered tax loop")
			//---------Other charges/Taxes----------
			log.Println("Entered PO Create Othercharges")
			miscpodetid := "MISCID-"+strconv.Itoa(miscids)			
			sqlStatementOT1:=`INSERT INTO dbo.pur_gc_po_details_taxes_newpg(
					taxid, itemid, pono, cid, idsno, perc, isreceivable, poid)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`
			rows, err = db.Query(sqlStatementOT1,
									tax.TaxId,
									po.ItemID,
									po.PoNO,
									po.Contract,
									taxids,
									tax.TaxPercentage,
									true,
									po.PoId)
			if err != nil {
				log.Println(err)
				return events.APIGatewayProxyResponse{500, headers, nil, "error while inserting tax details", false}, nil
			}
			sqlStatementOT2:=`INSERT INTO dbo.pur_gc_po_misc_details_newpg(
				podetid, podetidsno, miscid, poid, rate,createduserid, createddate,taxid)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`
			rows, err = db.Query(sqlStatementOT2,
									miscpodetid,
									miscids,
									tax.MiscId,
									po.PoId,
									tax.MiscRate,
									po.CreatedUserID,
									po.PoDate,
									tax.TaxId)	
			if err != nil {
				log.Println(err)
				return events.APIGatewayProxyResponse{500, headers, nil, "error while inserting tax details", false}, nil
			}
			miscids=miscids+1
			taxids=taxids+1
		}

		

		// Insert Audit Info.
		log.Println("Entered Audit Module for PO Type")
		// Find created user username
		sqlStatementAUser1 := `SELECT u.userid 
								FROM dbo.users_master_newpg u
								inner join dbo.pur_gc_po_con_master_newpg po on po.createdby=u.userid
								where po.pono=$1`
		rows, err = db.Query(sqlStatementAUser1, po.PoNO)
		for rows.Next() {
			err = rows.Scan(&po.GCCreatedUserID)
		}
		audit.CreatedUserid = po.GCCreatedUserID
		audit.CreatedDate = po.PoDate
		audit.Description = "PO Modified"
		// sd.InvoiceDate = time.Now().Format("2006-01-02")
		audit.ModifiedDate = time.Now().Format("2006-01-02")
		audit.ModifiedUserid = po.CreatedUserID

		sqlStatementADT := `INSERT INTO dbo.auditlog_pur_gc_master_newpg(
						pono,createdby, created_date, description,modifiedby, modified_date)
						VALUES($1,$2,$3,$4,$5,$6)`
		_, errADT := db.Query(sqlStatementADT,
			po.PoNO,
			audit.CreatedUserid,
			audit.CreatedDate,
			audit.Description,
			audit.ModifiedUserid,
			audit.ModifiedDate)

		log.Println("Audit Insert Query Executed")
		if errADT != nil {
			log.Println("unable to insert Audit Details", errADT)
		}

	}

	return events.APIGatewayProxyResponse{200, headers, nil, string("success"), false}, nil
}

func main() {
	lambda.Start(editGCPODetails)
}
func findLatestSerial(param1, param2, param3, param4 string) (ids int) {
	log.Println("Finding latest serial num")
	db, _ := sql.Open("postgres", PsqlInfo)
	defer db.Close()
	var rows *sql.Rows
	sqlStatement1 := fmt.Sprintf("SELECT %s FROM %s where %s is not null ORDER BY %s DESC LIMIT 1", param1, param2, param3, param4)
	rows, err := db.Query(sqlStatement1)
	for rows.Next() {
		err = rows.Scan(&ids)
	}
	if err != nil {
		log.Println(err)
	}
	return ids + 1
}
func deleteRecords(param1, param2 string) (result string) {
	log.Println("deleting records")
	db, _ := sql.Open("postgres", PsqlInfo)
	defer db.Close()
	
	sqlStatement2 := fmt.Sprintf("delete from %s where pono=$1", param1)
	_, err := db.Query(sqlStatement2,param2)
	if err != nil {
		log.Println(err)
		// return err.Error()
	}
	return "success"
}
func insertDomesticTax(param1, param2, param3, param4,param5,param6 string) (status string) {
	log.Println("Inserting tax")
	db, _ := sql.Open("postgres", PsqlInfo)
	defer db.Close()
	deleteRecords("dbo.pur_gc_po_details_taxes_newpg",po.PoNO)
	taxidsno:=findLatestSerial("idsno","dbo.pur_gc_po_details_taxes_newpg","idsno","idsno")
	taxdetid:="DTAX-"+strconv.Itoa(taxidsno)
	sqlStatementIT := `INSERT INTO dbo.pur_gc_po_details_taxes_newpg
		(taxid,taxname, itemid, pono,perc,poid,taxdetid,idsno)
		VALUES ($1, $2, $3, $4, $5, $6,$7,$8)`
	log.Println(sqlStatementIT,param1, param2, param3, param4,param5,param6,taxdetid,taxidsno)
	_, err := db.Query(sqlStatementIT,param1, param2, param3, param4,param5,param6,taxdetid,taxidsno)	
	if err != nil {
		log.Println(err)
	}
	return "success"
}
