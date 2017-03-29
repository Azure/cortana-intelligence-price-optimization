Login-AzureRmAccount

Get-AzureRmSubscription


$subscriptionName = Read-Host 'Enter the Subscription Name?'
$certName ="retailopt"+[guid]::NewGuid().toString().subString(0,8)
$certPassword = Read-Host -Prompt 'Please enter a new Password here, which will be used as the password for the certificate to be created?' 
$clusterName = $certName

$ErrorActionPreference = "Stop"
$VerbosePreference = "Continue"

try{

Get-AzureRmSubscription -SubscriptionName $subscriptionName | Select-AzureRmSubscription

#### 2. Create a PFX certificate
#### On your Microsoft Windows machine's PowerShell, run the following command to create your certificate.  

$certFolder =  get-location
$certFilePath = "$certFolder\certFile.pfx"
$certStartDate = (Get-Date).Date
$certStartDateStr = $certStartDate.ToString("MM/dd/yyyy")
$certEndDate = $certStartDate.AddYears(1)
$certEndDateStr = $certEndDate.ToString("MM/dd/yyyy")


$certPasswordSecureString = ConvertTo-SecureString $certPassword -AsPlainText -Force
#mkdir -Force $certFolder
$cert = New-SelfSignedCertificate -DnsName $certName -CertStoreLocation cert:\CurrentUser\My -KeySpec KeyExchange -NotAfter $certEndDate -NotBefore $certStartDate
$certThumbprint = $cert.Thumbprint
$cert = (Get-ChildItem -Path cert:\CurrentUser\My\$certThumbprint)
Export-PfxCertificate -Cert $cert -FilePath $certFilePath -Password $certPasswordSecureString


#### 3. Create the Service Principal Identity (SPI)  
##### 1. Using the earlier created certificate, create your SPI.
$certificatePFX = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2($certFilePath, $certPasswordSecureString)
$credential = [System.Convert]::ToBase64String($certificatePFX.GetRawCertData())


##### 2. Use this cmdlet, if you installed Azure PowerShell 2.0 (i.e After August 2016)
$application = New-AzureRmADApplication -DisplayName $certName -HomePage "https://$clusterName.azurehdinsight.net" -IdentifierUris "https://$clusterName.azurehdinsight.net"  -CertValue $credential -StartDate $certStartDate -EndDate $certEndDate


##### 3. Use this cmdlet, if you installed Azure PowerShell 1.0
# $application = New-AzureRmADApplication -DisplayName $certName -HomePage "https://$clusterName.azurehdinsight.net" -IdentifierUris "https://$clusterName.azurehdinsight.net"  -KeyValue $credential -KeyType "AsymmetricX509Cert" -KeyUsage "Verify"  -StartDate $certStartDate -EndDate $certEndDate

#Retrieve the Service Principal details
$servicePrincipal = New-AzureRmADServicePrincipal -ApplicationId $application.ApplicationId

#### 4. Retrieve Service Principal information needed for HDInsight(Spark) deployment

$tenant_ID = (Get-AzureRmContext).Tenant.TenantId

$object_ID = $servicePrincipal.Id

$application_ID = $servicePrincipal.ApplicationId

$cert_Value = [System.Convert]::ToBase64String((Get-Content $certFilePath -Encoding Byte))

$OFS = "`r`n`r`n"

Set-Content $certFolder\Demo_Inputs_$certName.txt "Demand Forecasting and Price Optimization for Retail Solution"
Add-Content $certFolder\Demo_Inputs_$certName.txt "$OFS You will be asked to input below parameters in the next deployment screen. Make sure you do not include any extra space while copying the passwords/certificate value"

Add-Content $certFolder\Demo_Inputs_$certName.txt "$OFS Service Principal Name: $certName"
Add-Content $certFolder\Demo_Inputs_$certName.txt "$OFS Tenant ID: $tenant_ID"
Add-Content $certFolder\Demo_Inputs_$certName.txt "$OFS Object ID: $object_ID"
Add-Content $certFolder\Demo_Inputs_$certName.txt "$OFS Application ID: $application_ID"
Add-Content $certFolder\Demo_Inputs_$certName.txt "$OFS Certificate Content(Base-64): $cert_Value"
Add-Content $certFolder\Demo_Inputs_$certName.txt "$OFS Certificate Password: $certPassword"
Add-Content $certFolder\Demo_Inputs_$certName.txt "$OFS Client Secret: get client secret from step B"

Write-Output "Text file location:"$certFolder"\Demo_Inputs_"$certName".txt"
    
Start-Process $certFolder\Demo_Inputs_$certName.txt
}
catch{
$Error[0]
}
