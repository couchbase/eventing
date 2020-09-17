## About
This demo shows how to use eventing to send an email if a package misses committed delivery date.

## Insturctions
1. Generate some sample data by running 'go run \*.go -args 10 > data.json' (generates 10 docs)
1. Create two buckets, one called labels (to hold the dat) and another called meta (for eventing metadata)
1. Import the Handler\_EmailOnDelay.json file
1. Put in a suitable SendGrid API key into the curl binding 'mailer\_binding'
1. Change the email address in the handler to your own email
1. Import it using: cbimport json -c localhost -u Administrator -p password -b labels -f list -d file://./data.json -g '%number%'
1. Deploy the handler
1. Now open a document with exceptions in labels bucket, and delete the 'delivered' field
1. At the promised delivery date, an email will be sent indicating delay

## Sample Data
Run main.go to generate data. Below is a sample document:
``` js
{
  "number": "1Z 174 JP7 03 4463 8651",
  "sender_name": "Mindi Oatley",
  "sender_company": "LegiNation, Company",
  "sender_addr": {
    "street": "3123 Mallorca Way",
    "city": "Howell",
    "state": "NJ",
    "zip": "07010-4725"
  },
  "declared_value": 0,
  "description": "2010 Ford Explorer Xlt 4dr, 2wd, Suv Mid Size",
  "receiver_name": "Soo Benett",
  "receiver_company": "DemystData Inc",
  "receiver_addr": {
    "street": "394 Hulbert Aly",
    "city": "Rochester",
    "state": "NY",
    "zip": "14558-0612"
  },
  "weight_lb": 0.1,
  "dimensions_in": "15 x 9.5 x 0.1",
  "created": "05/10/2019 10:15 AM",
  "picked_up": "05/12/2019 04:46 PM",
  "deliver_by": "05/12/2019 09:30 AM",
  "exceptions": [
    {
      "date": "05/12/2019 09:05 AM",
      "description": "THIS SHIPMENT WAS KEYED WITH THE INCORRECT SERVICE LEVEL AT THE ORIGIN ODC SITE"
    },
    {
      "date": "05/13/2019 05:51 PM",
      "description": "THE FREIGHT SHIPMENT HAS ARRIVED AT THE CARRIER DESTINATION FACILITY"
    },
    {
      "date": "05/13/2019 12:54 PM",
      "description": "PKG DELAYED - NO CONNECTION TO CUSTOMS"
    },
    {
      "date": "05/14/2019 01:43 PM",
      "description": "UNAUTHORIZED OVERWEIGHT/OVERSIZED SHIPMENT; PRIOR AUTHORIZATION REQUIRED"
    }
  ]
}
```

