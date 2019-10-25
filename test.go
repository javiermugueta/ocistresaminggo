package main
import (
	"context"
	"fmt"
	"github.com/oracle/oci-go-sdk/common"
	"github.com/oracle/oci-go-sdk/streaming"
	"github.com/google/uuid"
)

var region string = "eu-frankfurt-1"

/*
var tenencia string = "ocid1.tenancy.oc1..aaaaaaaacrhnntnyhzcz7nzaypeprc2hmgzkze4kgvwi75ctnykad7lunnba"
var user string = "ocid1.user.oc1..aaaaaaaacdgrqikjbtujaemh4p6yclymoe5cbw5jcp7obv6mzx5ly3sfhwta"
var huelladactilar string = "2b:c4:ae:d2:df:40:ec:33:22:43:36:c7:aa:d9:73:3c"
var privatekey string = "-----BEGIN RSA PRIVATE KEY-----MIIEpAIBAAKCAQEAtpiguwaAsc2JHfPtoSR9UMO3m0totO+2+PeA1HgvlCBljtOi1BHLzQIdBPDGbNUSyYhVtHAArhjlskVbC6AIN4DEjKUFci9IGNeWpooBzyAu8LeXQ1Ebg6xnWLKLQp6u/TzsOvUyQn15rXg7OmvjNJ2XaC0YMO/VxiAaP1pMs9Wkj8kMgUDDOP0h9gCl/ugwO1wKj0Ui/hMcGRdPYAQJhCpZLz5fKTPRdELWSqM3Lk6a63lNNRcc5WbTUnyN19yKPrYTMjRn0vlHzOUxJ/ZixXfVpPX5cyefMrb7KbFQJzwPcB02F1kwa7EIm1FuHcph8/Pl/VGsjs7GRRvIKNGPjQIDAQABAoIBAQCSXXPxlvvQCpZPyTkBA5PkCo6B0AelicWceiDtr01MKk/eFi0eWwmHUptaW3piwMVngH5avbD93P06Ujfx8JzFiNWEe78K0XW0XBPpeRSMKBsYX8HDdx9cA/VvducERti0K4Kcm/FmtNe1QnwPj223vl8gZY1PpOAyCYegosx7FXoHYPpzkpH5rA2PSwDAfXDIdfMC7TwTa9YeLgsFcuhuEsyB2g6QsoEBozdmkgwormvLSRpsiAyFE2oschXFi+66NZ7XxGEd1mg2yTdaRbi1u8492SecLeA1YCOJfzf+Q++X87hF58cxWUyPgylLWHDcAYWRCThGTRIewQ9YN2nBAoGBAOEToZUDdlW+Vz/ZWF5I2sU5FfhbXqgT8Rnd3Pokxdw50Xrugg3WgRvuILHCKPj7RqTQzxIirYbuupA975cZ1dBQnQezEJ4Pf64wJOjMdy9dVEPZk3PnEJ3QEmYAlMQ+piTQGFtCrvgNzqtV1i/pesqqk6mSFUjsXl5bQhCQNR9RAoGBAM+u4OZbceAZRyOG8UAHWFEPWYC50ojPPkyp1lrZBT1pKyaJpQRoI7amIlUjg41NH7XtLz/BvErRv+FJezXA7V69EU5eYa1rn+SQn+onEQi7CH/ZMr+H68TmGoNEq5SFmR5sDibRuKdme5/oUD5S3eAjQGglyj1TB6ByjjAmrbV9AoGAOeuUXEV80UnfKAYlcHXtHm23Uqcor7YaCF8Iu7J03FxgpPL1stvtc6OO9E+TvabkIgu+DduwrhWHGxFlM4wpcqccEdwpvZMdd1TaWrIrRZwN0DwQbvYaV5Iw+eDSZ8H7fWOzsOBzKFBdS8gWC1RZDghhYXS/V3nEOyHe2WZS1VECgYB/TyZST5AY3aX8I0ZUB6yd1Bj7je1/K/t2p03dVtedc5CeCRZ9AxGRb3nwUtrbjYF41jJ2lN5FxxomkhLTOrbHsoKfVb/uvODBDd7ZfKU2guCM7qnrOvjONHfgLAI6A0N/oKF0Lm9RPsNdnN1DtyyHr1RWA4Rb3z/3nQGIhs1JSQKBgQCR8ntYbBAOyP9Jxh8CbbCl/oJvtDHqtgLszDNo9fp0oMkP2YJr8Izjam49wfGcTKgU7zKHIPClSoM+LLcXpucVQJPNNiHyGICNNMKnI7A0+iV84qFkioJGCB++l+sC5vAPwE7OYGNweM76omzy+yuHa7o128SLXPNVc50bmNWK+g==-----END RSA PRIVATE KEY-----"
var secret string = ""
var psecret *string = &secret
*/

func main() {
	//cp := common.NewRawConfigurationProvider(tenencia, user, region, huelladactilar, privatekey, psecret)
	//fmt.Println("cp:", cp)
	//client, err := streaming.NewStreamClientWithConfigurationProvider(cp)
	client, err := streaming.NewStreamClientWithConfigurationProvider(common.DefaultConfigProvider())
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	var frastream string = "ocid1.stream.oc1.eu-frankfurt-1.aaaaaaaao367olzjaxqejgoxdr4x7tzpygjxubhzcmjpak2d7qrpzipdqvja"
	
	for i:= 0; i < 100 ;i++{
		putMessage(client, frastream, region, uuid.New().String(), fmt.Sprintf("{a:%d,b:{x:a,y:prueba}}", i))
	}
	getMessage(client, frastream, region, 999, "TRIM_HORIZON", "0")
	return
}
/*
* put a meessage in a topic
*
*/
func putMessage(client streaming.StreamClient, stream string, region string, key string, value string) int{
	var req streaming.PutMessagesRequest
	req.StreamId = &stream
	var entry streaming.PutMessagesDetailsEntry
	entry.Key = []byte(key)
	entry.Value = []byte(value)
	var entryarray [] streaming.PutMessagesDetailsEntry
	entryarray = append(entryarray, entry)
	var det streaming.PutMessagesDetails
	det.Messages = entryarray
	req.PutMessagesDetails = det
	client.SetRegion(region)
	_, err := client.PutMessages(context.Background(), req)
	if err != nil {
		fmt.Println("Error:", err)
		return  -1
	}
	return  0
}
/*
* gets messages from a topic
*
*
*/
func getMessage(client streaming.StreamClient, stream string, region string, limit int, mode string, partition string) (int, string){
	var req streaming.CreateCursorRequest
	req.StreamId = &stream
	req.Type = streaming.CreateCursorDetailsTypeEnum(mode)
	req.Partition = &partition
	var det streaming.CreateCursorDetails
	det.Partition = &stream
	cursorresponse, err := client.CreateCursor(context.Background(), req)
	if err != nil {
		fmt.Println("Error:", err)
		return  -1, ""
	}
	var msgreq streaming.GetMessagesRequest 
	msgreq.Cursor = cursorresponse.Cursor.Value
	msgreq.StreamId = &stream
	msgreq.Limit = &limit
	client.SetRegion(region)
	resp, err := client.GetMessages(context.Background(), msgreq)
	for i, v := range resp.Items {
		fmt.Printf("%d %v %v\n", i, v.Key, string(v.Value))
	}
	return 0, ""
}