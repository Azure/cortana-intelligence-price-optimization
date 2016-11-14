# cortana-intelligence-retail-price-optimization
Pricing has been growing as an overwhelming task for lots of retailers nowadays due to the expanding assortments. One of the greatest advantage of the data explosion era is that: huge amount of historical transactional data, which contains rich information about price elasticity of all different kinds of products and various potential drivers for demand change, are available to be utilized for a better and more efficient pricing strategy. 
As a result, questions come out such as: How could the insights in the large volume of data be leveraged to daily pricing task? What analytical approach should be taken to optimize the price? How to operationalize the analytical solution in a regular time based schedule? And how could the solution be validated properly? 
This solution provided here gives a potential way to address the questions raised above. In this solution, historical transactional data are utilized for building the demand forecasting model, which lend insights to critical patterns such as: How will changes of prices on one particular product impact the sales of itself as well as the sales of other products in the same competing group? How will other potential attributes such as brand, department and store attributes impact the price elasticity properties of a particular product as well as the sales? After learning the patterns, future demand for a set of potential price choices are predicted. And the optimization algorithm, which aimed to maximize total profit, will takes the predictions as inputs, and solves multiple mixed integer programming problems to find the optimal prices. And the whole process described above are operationalized in Cortana Intelligence Suite 
## Analytical Approach
In this session, we explain more details about the analytical approach we took here. 
### Demand Forecasting 

### Price Optimization

test test## Solution Architecture
### General Overview of Solution Architecture
### About Implementation on Spark

## References
[1] Ferreira, Kris Johnson, Bin Hong Alex Lee, and David Simchi-Levi. "Analytics for an online retailer: Demand forecasting and price optimization." Manufacturing & Service Operations Management 18.1 (2015): 69-88.
