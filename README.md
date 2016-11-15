# Retail Pirce Optimization Solution in Cortana Intelligence Suite
Pricing has been growing as an overwhelming task for lots of retailers nowadays due to the expanding assortments. One of the greatest advantage of the data explosion era is that: huge amount of historical transactional data, which contains rich information about price elasticity of all different kinds of products and various potential drivers for demand change, are available to be utilized for a better and more efficient pricing strategy. 

However, challenges always come with opportunities: How could the insights in the large volume of data be leveraged to daily pricing task? What analytical approach should be taken to optimize the price? How to operationalize the analytical solution in a regular time based schedule? And how could the solution be validated properly? 

This solution provided here gives a potential way to address the questions raised above. In this solution, historical transactional data are utilized for building the demand forecasting model, which lend insights to patterns such as: How will changes of prices on one particular product impact the 
sales of itself as well as the sales of other products in the same competing group? How will other potential attributes such as brand, department and store attributes impact the price elasticity properties of a particular product as well as the sales? After learning the patterns, future demands for a set of potential price choices are predicted. And the optimization algorithm, which aimed to maximize total profit, will takes the predictions as inputs, and solves multiple mixed integer programming problems to find the optimal prices. The whole process described above are operationalized in Cortana Intelligence Suite. 

The resulted solution will enable retailers to get recommended optimal prices at a regular-time bases, achieve better profitability, and save the routine human and time resources allocated on pricing tasks.

## Analytical Approach
In this session, we explain more details about the analytical approach taken in the solution. The price optimization approach and algorithm used in this solution references the research work by Kris Johnson Ferreira, etc [1]. 
### Demand Forecasting

The following driving factors for demand are considered in demand forecasting modelling: 

(1) Price-related features such as the actual sales price, the discount of the price compared to MSRP (manufacturer’s suggested retail price), and the relative price which measures how expensive is this product compared to other products in the same group, calculated as the ratio of price of this product to the average price of all products in the same competing group. 

(2) Products attributes such as the brand and department information.

(3) Store attributes such as average traffic in the store, etc. 

Demand forecasting tree models are built on features mentioned above. The performances of the models are evaluated through mean absolute percentage error. The model is retrained monthly, since more historical data will improve the prediction accuracy and indeed improve the optimization results.

### Price Optimization
In this solution, prices are optimized within a competing group. That is to say, for each competing group, one integral optimization problem is solved, and will give optimal prices for all products in this competing group. A competing group contains similar products that are competing against each other on the market, thus price changes on one product will affect the sales of the other products in the same competing group, whereas price changes on products from other competing groups will have minor impact on the sales of products in this competing group. And each integral optimization problem for an individual competing group mentioned above is a mixed integer problem, and is finally solved through solving a series of linear and integer programming problems [1]. 

While different retailers could vary a lot on how to define the competing group, here in this solution, as a general starting example, the competing group is defined as products within the same department and sold at the same store. As a result, different stores could price the same product differently in the solution, which corresponds to the reality that retailers could have different pricing or promotional strategies for different stores with various store attributes such as average traffic, store location and store tier. Besides, different retailers could also vary a lot on the pricing change schedule. Again, as a general starting example, the price is optimized weekly based on the weekly demand prediction results.

## Solution Architecture
In this session, we explain more details about how the above analytical approach is operationalized in Cortana Intelligence Suite.
### What’s Under the Hood
Raw simulated transactional data are pushed into Azure Data Lake Storage, from where the Spark Jobs ran on HDInsight Cluster will take the raw data as inputs and: (1) Turn the unstructured raw data into structured data and aggregate the individual transactions into weekly sales data. (2) Train demand forecasting model on the aggregated sales data. (3) Run the optimization algorithm and return the optimal prices for all products in all competing groups. The final results are visualized in Power BI Dashboard. And the whole process is scheduled weekly, the data movement and scheduling are managed by Azure Data Factory.
### About Implementation on Spark
A parallel version of the price optimization algorithm is implemented on Spark. Utilizing the RDD.map(), the independent price optimization problems for products in different competing group can be solved in parallelism and thus save the computational time.
## References
[1] Ferreira, Kris Johnson, Bin Hong Alex Lee, and David Simchi-Levi. "Analytics for an online retailer: Demand forecasting and price optimization." Manufacturing & Service Operations Management 18.1 (2015): 69-88.
