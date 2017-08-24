<?php

$sql = "
	SELECT
		processed,
		processedDate,
		processedQty,
		title,
		billerId,
		transactionCharge,
		monthlyCharge
	FROM
		prepaid_configuration 
WHERE processed = 1 AND processedQty > 0
	";

$prePaidConfig = $server->fetchResultSet($sql);

foreach($prePaidConfig as $profile)
	runPPCards($profile);

$sql = "
	SELECT
		PC.prePaidId,
		PC.customerId,
		PC.cardNumber,
		PC.cardType,
		PC.cardBin,
		PC.cardLast4,
		PC.cardSecurityCode,
		PC.cardExpiryDate,
		PC.cardLimit,
		PC.paySourceId
	FROM
		prepaid_cards PC
	";

$prePaidCards = $server->fetchResultSet($sql);

processTransaction($prePaidCards,$prePaidConfig);


function runPPCards($profile)
{
	$qty = $profile->processedQty;
	
	$sql = "
	SELECT
		PC.prePaidId,
		PC.customerId,
		PC.cardNumber,
		PC.cardType,
		PC.cardBin,
		PC.cardLast4,
		PC.cardSecurityCode,
		PC.cardExpiryDate,
		PC.cardLimit,
		PC.paySourceId,
		O.customerId,
		O.firstName,
		O.lastName,
		O.address1,
		O.address2,
		O.city,
		O.state,
		O.postalCode,
		O.country,
		O.shipFirstName,
		O.shipLastName,
		O.shipAddress1,
		O.shipAddress2,
		O.shipCity,
		O.shipState,
		O.shipPostalCode,
		O.shipCountry,
		O.phoneNumber,
		O.emailAddress
	FROM
		prepaid_cards PC
LEFT JOIN customers C ON C.customerId = PC.customerId
LEFT JOIN transactions T ON T.customerId = C.customerId
LEFT JOIN orders O ON O.orderId = T.orderId
WHERE NOT (O.orderId IS NOT NULL AND T.responseType = 'SUCCESS' AND O.orderType = 'NEW_SALE')
GROUP BY PC.prePaidId
LIMIT {$qty}
	";
	$server = Application::getDataServer();	
	$cards = $server->fetchResultSet($sql);
	
	foreach($cards as $card)
	{

		if(empty($card->customerId))
			attachCard($card);
			
		$transaction = new Transaction;
		$transaction->totalAmount = $profile->transactionCharge;
		$transaction->currencyCode = 'USD';
		$transaction->billerId = $profile->billerId;
		$transaction->transactionType = 'SALE';
		$transaction->paySource = 'CREDITCARD';
		$transaction->clientOrderId = strings::randomHex(14);
		objects::copyProps($card,$transaction);
	
		tools::dumpVar($transaction);
		
		$transaction->cardExpiryDate = new DateTime($transaction->cardExpiryDate);
		$merchant = MerchantAccount::fetch((int) $profile->billerId);
		$merchant->executeTransaction($transaction,$transaction);
	
		$transaction->create();
		
	}
}

function processTransaction($prePaidCards,$prePaidConfig)
{
	
	$server = Application::getDataServer();	
				
	$sql = "
	SELECT
		count(*)
	FROM
		prepaid_cards
	WHERE customerId IS NOT NULL
	";
	
	$prePaidQty = $server->fetchValue($sql);
	
	foreach($prePaidConfig as $k=>$v)
	{
		if($v->processedQty > $prePaidQty)
		{
			echo "$v->title is not able to process $v->processedQty transactions due to insufficient pre paid cards inside of Pre-Paid Portal.";
			unset($prePaidConfig[$k]);
		}			
	}
		
	foreach($prePaidConfig as $profile)
	{

		if($profile->processed == '1')
		{
			
			foreach($prePaidCards as $k=>$v)
				if($profile->transactionCharge > $v->cardLimit)
					unset($prePaidCards[$k]);
			
			foreach($prePaidCards as $card)
				if(empty($card->customerId))
					unset($prePaidCards[$k]);
			
			foreach($prePaidCards as $card)
			{
				
				$sql = "
				SELECT
					O.orderStatus,
					O.orderId,
					O.customerId,
					O.firstName,
					O.lastName,
					O.address1,
					O.address2,
					O.city,
					O.state,
					O.postalCode,
					O.country,
					O.shipFirstName,
					O.shipLastName,
					O.shipAddress1,
					O.shipAddress2,
					O.shipCity,
					O.shipState,
					O.shipPostalCode,
					O.shipCountry,
					O.phoneNumber,
					O.emailAddress,
					O.cardType,
					O.cardNumber,
					O.cardSecurityCode,
					O.cardExpiryDate,
					C.primaryPaySourceId
				FROM
					orders O
				INNER JOIN customers C ON C.customerId = O.customerId
				WHERE 
					O.customerId = ? AND 
					C.primaryPaySourceId = ? AND 
					O.orderStatus = 'PARTIAL' AND
					O.firstName IS NOT NULL AND
					O.lastName IS NOT NULL AND
					O.address1 IS NOT NULL AND
					O.city IS NOT NULL AND
					O.state IS NOT NULL AND
					O.postalCode IS NOT NULL AND
					O.country IS NOT NULL
				LIMIT 1
				";
				
				$order = $server->fetchResultSet($sql,$card->customerId,$card->paySourceId);
				$order = (object) $order[0];
				
				tools::dumpVar($order);
				
				$ppOrder = new CustomerOrder;
				$ppOrder->orderType = 'PREPAID_SALE';
				$ppOrder->customerId = $order->customerId;
				$ppOrder->firstName = $order->firstName;
				$ppOrder->lastName = $order->lastName;
				$ppOrder->address1 = empty($order->address1) ? 'noship' : $order->address1;
				$ppOrder->address2 = $order->address2;
				$ppOrder->city = empty($order->city) ? 'nocity' : $order->city;
				$ppOrder->state = empty($order->state) ? 'GA' : $order->state;
				$ppOrder->postalCode = empty($order->postalCode) ? '33333' : $order->postalCode;
				$ppOrder->country = empty($order->country) ? 'US' : $order->country;
				$ppOrder->phoneNumber = $order->phoneNumber;
				$ppOrder->emailAddress = $order->emailAddress;
				$ppOrder->cardType = $card->cardType;
				$ppOrder->cardNumber = $card->cardNumber;
				$ppOrder->cardLast4 = substr($card->cardNumber,-4);
				$ppOrder->paySource = 'PREPAID';
				$ppOrder->campaignId = '';
				$ppOrder->totalAmount = $profile->transactionCharge;
				
				$ppOrder->create();
				
				dump_var($ppOrder);
												
			}
		}
	}
}

function attachPrePaidCards($prePaidCards)
{
	
	foreach($prePaidCards as $k=>$v)
		if(empty($v->customerId))
			attachCard($v);
}

function attachCard($card)
{
	
	$server = Application::getDataServer();
	
	if(!empty($card->customerId))
		return;
		
	$sql = "
	SELECT
		C.customerId,
		O.firstName,
		O.lastName,
		O.emailAddress,
		O.phoneNumber,
		O.address1,
		O.city,
		O.state,
		O.country,
		O.postalCode
	FROM 
		orders O
	INNER JOIN customers C ON C.customerId = O.customerId
	LEFT JOIN transactions T ON T.customerId = O.customerId
	WHERE NOT (O.orderId IS NOT NULL AND T.responseType = 'SUCCESS' AND O.orderType = 'NEW_SALE')
	LIMIT 1
	";
	
	$server->execute("INSERT INTO prepaid_cards (customerId) VALUES (?) WHERE prePaidId = ?", $partial->customerId, $card->prePaidId);			
}
