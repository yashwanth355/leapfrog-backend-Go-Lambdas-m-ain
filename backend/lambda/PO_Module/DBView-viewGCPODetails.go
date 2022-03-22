//updated total_quantity-Sep1
//updated audit query-Sep6
//updated fixationdate, new fields-Sep17
//Updated GC & supplier names- Sep23
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	// "strconv"
	// "time"

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
	Status          string `json:"status"`
	CreatedUserID   string `json:"createduserid"`
	GCCreatedUserID string `json:"gccreateduserid"`
	GCCoffeeType    string `json:"coffee_type"'`
	Type            string `json:"type"`
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
	POBillComName  string `json:"billing_com_name"`
	POBillAddress1 string `json:"billing_at_addressline1"`
	POBillAddress2 string `json:"billing_at_addressline2"`
	POBillState    string `json:"billing_state"`
	POBillCountry  string `json:"billing_country"`
	POBillZipCode  string `json:"billing_zipcode"`
	POBillGSTNo    string `json:"billing_at_gstno"`
	POBillPanNo    string `json:"billing_at_panno"`
	PODelTypeID    string `json:"delivery_at_id"`
	PODelTypeName  string `json:"delivery_at_name"`
	PODelAddress1  string `json:"delivery_at_addressline1"`
	PODelAddress2  string `json:"delivery_at_addressline2"`
	PODelState     string `json:"delivery_state"`
	PODelCountry   string `json:"delivery_country"`
	PODelZipCode   string `json:"delivery_zipcode"`
	PODelComName   string `json:"delivery_com_name"`
	PODelGSTNo     string `json:"delivery_at_gstno"`
	PODelPanNo     string `json:"delivery_at_panno"`

	//Green Coffee Info Section-Done--------------------------

	ItemID        string `json:"item_id"`
	ItemName      string `json:"item_name"`
	TotalQuantity string `json:"total_quantity"`
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

	PurchaseType       string `json:"purchase_type"`
	TerminalMonth      string `json:"terminal_month"`
	BookedTerminalRate string `json:"booked_terminal_rate"`
	BookedDifferential string `json:"booked_differential"`
	FixedTerminalRate  string `json:"fixed_terminal_rate"`
	FixedDifferential  string `json:"fixed_differential"`
	PurchasePrice      string `json:"purchase_price"`
	MarketPrice        string `json:"market_price"`
	POMargin           string `json:"po_margin"`
	// FinalPrice			 string `json:"final_price"`

	Advance     string `json:"advance"`      //changed
	AdvanceType string `json:"advance_type"` //changed
	PoQty       string `json:"po_qty"`
	// Price 				 string `json:"price"`

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
	Tds  string `json:"tds"`
	//domestic section
	PurchasePriceInr string `json:"purchasePriceInr"`
	MarketPriceInr   string `json:"marketPriceInr"`
	FinalPriceInr    string `json:"finalPriceInr"`
	DTerminalPrice   string `json:"terminalPrice"`
	TotalPrice       string `json:"totalPrice"`
	//Other Information
	TaxDuties        string `json:"taxes_duties"`
	ModeOfTransport  string `json:"mode_of_transport"`
	TransitInsurance string `json:"transit_insurance"`
	PackForward      string `json:"packing_forwarding"`
	//Other charges
	OtherCharges    string         `json:"otherCharges"`
	Rate            string         `json:"rate"`
	GrossPrice      string         `json:"grossPrice"`
	AuditLogDetails []AuditLogGCPO `json:"audit_log_gc_po"`
	//Consolidated Finance

	QCStatus      string `json:"qcStatus"`
	APStatus      string `json:"apStatus"`
	PayableAmount string `json:"payable_amount"`
	//new fields
	NoOfBags     string  `json:"no_of_bags"`
	NetWt        string  `json:"net_weight"`
	MTQuantity   float64 `json:"quantity_mt"`
	FixationDate string  `json:"fixation_date"`
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
}
type ItemDispatch struct {
	DispatchID        string `json:"dispatch_id"`
	DispatchQuantity  string `json:"dispatch_quantity"`
	DispatchDate      string `json:"dispatch_date"`
	DSNo              string `json:"number"`
	DDate             string `json:"date"`
	DeliveredQuantity string `json:"delivered_quantity"`
	BalanceQuantity   string `json:"balance_quantity"`
	RelatedDetid      string `json:"related_detid"`
}
type AuditLogGCPO struct {
	CreatedDate    string `json:"createddate"`
	CreatedUserid  string `json:"createduserid"`
	ModifiedDate   string `json:"modifieddate"`
	ModifiedUserid string `json:"modifieduserid"`
	Description    string `json:"description"`
}

func viewGCPODetails(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	headers := map[string]string{"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Headers": "Origin, X-Requested-With, Content-Type, Accept"}
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	var po PurchaseOrderDetails

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
	// var rows *sql.Rows

	if po.PoNO != "" {
		log.Println("Entered PO View Module")
		log.Println("selected PO NO:", po.PoNO)
		param:="dbo.viewpo order by pono desc limit 1"
		var incoterms,currencyname,currencycode,cat_type,vencountry,venaddress,venemail,payable_amount,poid,pocat,posubcat,vendorid,dispatchterms,origin,
		poloading,insurance,destination,forwarding,currencyid,nocontainers,payment_terms,remarks,payment_terms_days,billing_at_id,
		delivery_at_id,cid,status,quote_no,quote_date,quote_price,
		purchase_type,container_type,noofbags,accpay_status,qc_status,tds,transport_mode,advancetype,advance,
		rate,terminal_month,booked_term_rate,booked_differential,fixed_term_rate,
		fixed_differential,market_price,po_margin,total_price,terminal_price,gross_price,fixationdate,
		netweight,balance_quantity,purchase_price,total_quantity,destinationportname sql.NullString
		var poGrid []PurchaseOrderDetails
		sqlStatementPOV1 :=`select 
		poid,podate,pocat,
		posubcat,vendorid,dispatchterms,origin,poloading,
		insurance,destination,forwarding,currencyid,nocontainers,
		payment_terms,remarks,payment_terms_days,billing_at_id,delivery_at_id,
		cid, status, quote_no,
		quote_date,quote_price,container_type, noofbags,accpay_status,
		qc_status, tds, transport_mode,advancetype,advance,
		rate, terminal_month,booked_term_rate,booked_differential,fixed_term_rate,
		fixed_differential, market_price, po_margin, total_price, terminal_price,
		purchase_type, gross_price, fixationdate,netweight,quantity_mt,
		balance_quantity,purchase_price,total_quantity,destinationportname,payable_amount,
		itemid,itemname,cat_type,currencyname,currencycode,incoterms
		from %s`
		rows, err := db.Query(fmt.Sprintf(sqlStatementPOV1,param))
		log.Println("PO Master Query Executed")
		if err != nil {
			log.Println("Fetching PO Details from DB failed")
			log.Println(err.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}
		defer rows.Close()
		for rows.Next() {
			err = rows.Scan(&poid, &po.PoDate, &pocat,
				&posubcat, &vendorid, &dispatchterms, &origin,&poloading,
				&insurance, &destination, &forwarding, &currencyid, &nocontainers,
				&payment_terms, &remarks, &payment_terms_days, &billing_at_id,&delivery_at_id,
				&cid,&status, &quote_no,
				&quote_date,&quote_price,&container_type, &noofbags,&accpay_status,
				&qc_status, &tds, &transport_mode,&advancetype,&advance,
				&rate, &terminal_month,&booked_term_rate,&booked_differential,&fixed_term_rate,
				&fixed_differential, &market_price, &po_margin, &total_price, &terminal_price,
				&purchase_type, &gross_price, &fixationdate,&netweight,&po.MTQuantity,
				&balance_quantity,&purchase_price,&total_quantity,&destination,&destinationportname,&payable_amount,&po.ItemID,&po.ItemName,&cat_type,&currencyname,&currencycode,
				&incoterms)
				
				po.Contract = cid.String
			po.IncoTermsID = dispatchterms.String
			po.IncoTerms=incoterms.String
			po.Origin = origin.String
			po.PortOfLoad = poloading.String //still missing
			po.Insurance = insurance.String     //still missing
			po.DPortId = destination.String
			po.DPortName = destinationportname.String
			po.Forwarding = forwarding.String
			po.NoOfContainers = nocontainers.String
			po.ContainerType = container_type.String
			po.PaymentTerms = payment_terms.String
			po.NoOfBags = noofbags.String
			po.NetWt = netweight.String
			po.PurchaseType = purchase_type.String
			
			po.TerminalMonth = terminal_month.String

			po.BookedTerminalRate = booked_term_rate.String
			po.BookedDifferential = booked_differential.String
			po.FixedTerminalRate = fixed_term_rate.String
			po.FixedDifferential = fixed_differential.String
//
			po.MarketPrice = market_price.String
			po.POMargin = po_margin.String
			po.Tds = tds.String
			po.AdvanceType = advancetype.String
			po.Advance = advance.String
			po.PaymentTermsDays = payment_terms_days.String
			po.PurchasePriceInr = purchase_price.String
			po.MarketPriceInr = market_price.String
			// po.FinalPriceInr
			po.DTerminalPrice = terminal_price.String
			
			po.CurrencyCode=currencycode.String
			po.CurrencyName=currencyname.String
			po.PayableAmount=payable_amount.String
			poGrid=append(poGrid,po)

		}
			
			
			if po.POSubCategory == "Import" {
				po.SupplierType = "Import"
			} else {
				po.SupplierType = "Domestic"
			}
		
		//------COMMON to IMPORT && DOMESTIC-------//

		po.SupplierID = vendorid.String
		 
		
		po.POBillTypeID = billing_at_id.String
		po.PODelTypeID = delivery_at_id.String
		po.CurrencyID = currencyid.String
		po.Status = status.String
		po.Comments = remarks.String
		// po.TaxDuties = taxdut.String
		po.ModeOfTransport = transport_mode.String
		// po.TransitInsurance = transins.String
		po.PackForward = forwarding.String
		
		po.Rate = rate.String
		po.PurchasePrice = purchase_price.String
		po.TotalPrice = total_price.String
		po.GrossPrice = gross_price.String
		po.FixationDate = fixationdate.String
		po.PurchaseType = purchase_type.String
		po.TerminalMonth = terminal_month.String
		//---------------_Fetch Billing Address Info------------------------
		log.Println("Entered Billing Module")
		sqlStatementPOVB2 := `SELECT 
		                potypeid,
						initcap(bdi.potypename),
						initcap(bdi.potypefullname), initcap(bdi.addressline1), initcap(bdi.addressline2), initcap(bdi.state), initcap(bdi.country), bdi.zipcode, 'GST no: ' || bdi.gstno , 'PAN no: ' || bdi.panno 
						from dbo.pur_po_types bdi
						where 
						 bdi.potypeid=(select pom.billing_at_id from dbo.pur_gc_po_con_master_newpg pom where pom.pono=$1)`
		rowsb2, errb2 := db.Query(sqlStatementPOVB2, po.PoNO)
		log.Println("PO Types Query Executed")
		if errb2 != nil {
			log.Println("Issue in fetching billing address from DB failed")
			log.Println(errb2.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, errb2.Error(), false}, nil
		}

		var billPanNo, delPanNo, billCountry, delCountry sql.NullString
		defer rowsb2.Close()
		for rowsb2.Next() {
			errb2 = rowsb2.Scan(&po.POBillTypeID, &po.POBillTypeName, &po.POBillComName, &po.POBillAddress1, &po.POBillAddress2, &po.POBillState, &billCountry, &po.POBillZipCode, &po.POBillGSTNo, &billPanNo)
			po.POBillPanNo = billPanNo.String
			po.POBillCountry = billCountry.String
			log.Println(po.POBillAddress1)
			log.Println(po)
		}
		//---------------_Fetch Delivery Address Info------------------------
		log.Println("Entered PO Delivery Module")
		sqlStatementPOVD2 := `SELECT 
						  initcap(bdi.potypename),
						  initcap(bdi.potypefullname), initcap(bdi.addressline1), initcap(bdi.addressline2), initcap(bdi.state), initcap(bdi.country), bdi.zipcode, 'GST no: ' || bdi.gstno , 'PAN no: ' || bdi.panno 
						 from dbo.pur_po_types bdi
						 where 
						 bdi.potypeid=(select pom.delivery_at_id from dbo.pur_gc_po_con_master_newpg pom where pom.pono=$1)`
		rowsd2, errd2 := db.Query(sqlStatementPOVD2, po.PoNO)
		log.Println("PO Delivery Address Query Executed")
		if errd2 != nil {
			log.Println("Fetching PO Delivery Details from DB failed")
			log.Println(errd2.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}

		defer rowsd2.Close()
		for rowsd2.Next() {
			errd2 = rowsd2.Scan(&po.PODelTypeName, &po.PODelComName, &po.PODelAddress1, &po.PODelAddress2, &po.PODelState, &delCountry, &po.PODelZipCode, &po.PODelGSTNo, &delPanNo)
			po.PODelPanNo = delPanNo.String
			po.PODelCountry = delCountry.String
		}
		//-------__Fetch Vendor Information---------------------
		log.Println("Entered PO Vendor Module")
		sqlStatementPOV3 := `SELECT				
						vm.vendortypeid,
						vm.country,
						initcap(vm.vendorname),
						initcap(vm.address1)||','||initcap(vm.address2)||','||initcap(vm.city)||','||pincode||','||initcap(vm.state)||' -'||SUBSTRING (vm.gstin, 1 , 2)||','||'Phone:'||vm.phone||','||'Mobile:'||vm.mobile||','||'GST NO:'||vm.gstin||','||'PAN NO:'||vm.panno,
						vm.email
						from 
						dbo.pur_vendor_master_newpg vm
						where vm.vendorid=(select pom.vendorid from dbo.pur_gc_po_con_master_newpg pom where pom.pono=$1)`
		rows3, err3 := db.Query(sqlStatementPOV3, po.PoNO)
		log.Println("Vendor Details fetch Query Executed")
		if err3 != nil {
			log.Println("Fetching Vendor Details from DB failed")
			log.Println(err3.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}
		defer rows3.Close()
		for rows3.Next() {
			err3 = rows3.Scan(&po.SupplierTypeID, &vencountry, &po.SupplierName, &venaddress, &venemail)
		}
		po.SupplierCountry = vencountry.String
		po.SupplierAddress = venaddress.String
		po.SupplierEmail = venemail.String
		
		if po.AdvanceType == "101" {
			po.AdvanceType = "Percentage"
		} else {
			po.AdvanceType = "Amount"
		}
		

		
		
				
		
		

		// // ---------------------Fetch GC Composition Details--------------------------------------//
		log.Println("The GC Composition for the Item #", po.ItemID)
		sqlStatementPOGC1 := `SELECT density, moisture, browns, blacks, brokenbits, insectedbeans, bleached, husk, sticks, stones, beansretained
						FROM dbo.pur_gc_po_composition_master_newpg where itemid=$1`
		rows7, err7 := db.Query(sqlStatementPOGC1, po.ItemID)
		log.Println("GC Fetch Query Executed")
		if err7 != nil {
			log.Println("Fetching GC Composition Details from DB failed")
			log.Println(err7.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}

		for rows7.Next() {
			err7 = rows7.Scan(&po.Density, &po.Moisture, &po.Browns, &po.Blacks, &po.BrokenBits, &po.InsectedBeans, &po.Bleached, &po.Husk, &po.Sticks,
				&po.Stones, &po.BeansRetained)

		}

		//---------------------Fetch Multiple Dispatch Info-------------------------------------//
		//Old query:`SELECT detid,quantity,dispatch_type,dispatch_count,dispatch_date
		//from dbo.pur_gc_po_dispatch_master_newpg where pono=$1`
		log.Println("Fetching Single/Multiple Dispatch Information the Contract #")
		sqlStatementMDInfo1 := `select d.detid,d.dispatch_date,d.quantity, d.dispatch_type,d.dispatch_count,
							m.delivered_quantity, (m.expected_quantity-m.delivered_quantity) as balance_quantity,
							d.parent_detid
							from dbo.pur_gc_po_dispatch_master_newpg d
							left join dbo.inv_gc_po_mrin_master_newpg as m on m.detid=d.detid
							where d.pono=$1`
		rows9, err9 := db.Query(sqlStatementMDInfo1, po.PoNO)
		log.Println("Multi Dispatch Info Fetch Query Executed")
		if err9 != nil {
			log.Println("Multi Dispatch Info Fetch Query failed")
			log.Println(err9.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}
		var dispid, dispdate, dispquan, disptype, dispcoun, delquan, balquan, parentdetid sql.NullString
		for rows9.Next() {
			var mid ItemDispatch
			err9 = rows9.Scan(&dispid, &dispdate, &dispquan, &disptype, &dispcoun, &delquan, &balquan, &parentdetid)
			mid.DispatchID = dispid.String
			mid.DispatchDate = dispdate.String
			mid.DispatchQuantity = dispquan.String
			mid.RelatedDetid = parentdetid.String
			po.DispatchType = disptype.String
			po.DispatchCount = dispcoun.String
			mid.DeliveredQuantity = delquan.String
			mid.BalanceQuantity = balquan.String
			gcMultiDispatch := append(po.ItemDispatchDetails, mid)
			po.ItemDispatchDetails = gcMultiDispatch
			log.Println("added one")
			// po.DispatchType=mid.DispatchType
			// po.DispatchCount=mid.DispatchCount
		}
		log.Println("Multi Dispatch Details:", po.ItemDispatchDetails)

		//---------------Fetch Domestic Tax info for Domestic PO-------------------

		// if po.POSubCategory == "Domestic" {
		// 	log.Println("Selected supplier type Domestic Code:", po.POSubCategory)

		// 	var sgst, cgst, igst, packforward, install, freight, handling, misc, hamali, mandifee, fulltax, insurance sql.NullString
		// 	sqlStatementDTax1 := `SELECT sgst, cgst, igst,pack_forward, installation,
		// 					 freight, handling, misc, hamali, mandifee, full_tax,
		// 					  insurance FROM dbo.pur_gc_po_details_taxes_newpg 
		// 					  where pono=$1`
		// 	rowsDTax1, errDTax1 := db.Query(sqlStatementDTax1, po.PoNO)
		// 	log.Println("Domestic Tax Info Fetch Query Executed")
		// 	if errDTax1 != nil {
		// 		log.Println("Domestic Tax Info Fetch Query failed")
		// 		log.Println(errDTax1.Error())
		// 		return events.APIGatewayProxyResponse{500, headers, nil, errDTax1.Error(), false}, nil
		// 	}

		// 	defer rowsDTax1.Close()
		// 	for rowsDTax1.Next() {
		// 		errDTax1 = rowsDTax1.Scan(&sgst, &cgst, &igst, &packforward, &install, &freight,
		// 			&handling, &misc, &hamali, &mandifee, &fulltax, &insurance)
		// 	}
		// 	//Other Charges--Domestic
		// 	po.SGST = sgst.String
		// 	po.CGST = cgst.String
		// 	po.IGST = igst.String
		// 	po.DPackForward = packforward.String
		// 	po.DInstallation = install.String
		// 	po.DFreight = freight.String
		// 	po.DHandling = handling.String
		// 	po.DMisc = misc.String
		// 	po.DHamali = hamali.String
		// 	po.DMandiFee = mandifee.String
		// 	po.DFullTax = fulltax.String
		// 	po.DInsurance = insurance.String

		// }

		
		
		

		//---------------------Fetch Audit Log Info-------------------------------------//
		log.Println("Fetching Audit Log Info #")
		sqlStatementAI := `select u.username as createduser, gc.created_date,
			gc.description, v.username as modifieduser, gc.modified_date
   		from dbo.auditlog_pur_gc_master_newpg gc
   		inner join dbo.users_master_newpg u on gc.createdby=u.userid
  		 left join dbo.users_master_newpg v on gc.modifiedby=v.userid
   		where gc.pono=$1 order by logid desc limit 1`
		rowsAI, errAI := db.Query(sqlStatementAI, po.PoNO)
		log.Println("Audit Info Fetch Query Executed")
		if errAI != nil {
			log.Println("Audit Info Fetch Query failed")
			log.Println(err9.Error())
			return events.APIGatewayProxyResponse{500, headers, nil, err.Error(), false}, nil
		}

		for rowsAI.Next() {
			var al AuditLogGCPO
			errAI = rowsAI.Scan(&al.CreatedUserid, &al.CreatedDate, &al.Description, &al.ModifiedUserid, &al.ModifiedDate)
			auditDetails := append(po.AuditLogDetails, al)
			po.AuditLogDetails = auditDetails
			log.Println("added one")

		}
		log.Println("Audit Details:", po.AuditLogDetails)

		res, _ := json.Marshal(poGrid)
		return events.APIGatewayProxyResponse{200, headers, nil, string(res), false}, nil

	} else {
		return events.APIGatewayProxyResponse{200, headers, nil, string("Couldn't find PO Details"), false}, nil
	}
	return events.APIGatewayProxyResponse{200, headers, nil, string("success"), false}, nil
}

func main() {
	lambda.Start(viewGCPODetails)
}
