package lego.odata.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.olingo.commons.api.edm.EdmEnumType;
import org.apache.olingo.commons.api.edm.EdmType;
import org.apache.olingo.server.api.ODataApplicationException;
import org.apache.olingo.server.api.uri.UriInfoResource;
import org.apache.olingo.server.api.uri.UriResource;
import org.apache.olingo.server.api.uri.UriResourcePrimitiveProperty;
import org.apache.olingo.server.api.uri.UriResourceProperty;
import org.apache.olingo.server.api.uri.queryoption.expression.BinaryOperatorKind;
import org.apache.olingo.server.api.uri.queryoption.expression.Expression;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitException;
import org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitor;
import org.apache.olingo.server.api.uri.queryoption.expression.Literal;
import org.apache.olingo.server.api.uri.queryoption.expression.MethodKind;
import org.apache.olingo.server.api.uri.queryoption.expression.UnaryOperatorKind;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;

public class ElasticSearchExpressionVisitor implements ExpressionVisitor {

	public Object visitBinaryOperator(BinaryOperatorKind operator, Object left, Object right)
			throws ExpressionVisitException, ODataApplicationException {
		if (BinaryOperatorKind.EQ.equals(operator)){
			String fieldName = left.toString();
			Object value = right;
			if (value != null){
				return QueryBuilders.matchQuery(fieldName, value);
			}
			else{
				return QueryBuilders.filteredQuery(QueryBuilders
		                .matchAllQuery(), FilterBuilders.missingFilter(fieldName));
			}
		}
		return QueryBuilders.matchAllQuery();
	}

	public Object visitUnaryOperator(UnaryOperatorKind operator, Object operand)
			throws ExpressionVisitException, ODataApplicationException {
		// TODO Auto-generated method stub
		return null;
	}

	public Object visitMethodCall(MethodKind methodCall, List parameters)
			throws ExpressionVisitException, ODataApplicationException {
		// TODO Auto-generated method stub
		return null;
	}

	public Object visitLambdaExpression(String lambdaFunction, String lambdaVariable, Expression expression)
			throws ExpressionVisitException, ODataApplicationException {
		// TODO Auto-generated method stub
		return null;
	}

	public Object visitLiteral(Literal literal) throws ExpressionVisitException, ODataApplicationException {
		//return ODataQueryUtils.getRawValue(literal);
		return literal.getText();
	}

	public Object visitMember(UriInfoResource member) throws ExpressionVisitException, ODataApplicationException {
		if (member.getUriResourceParts().size() == 1) {
			UriResourcePrimitiveProperty property
	                                 = (UriResourcePrimitiveProperty)
	                                              member.getUriResourceParts().get(0);
	        return property.getProperty().getName();
	    } else {
	        List<String> propertyNames = new ArrayList<String>();
	        for (UriResource property : member.getUriResourceParts()) {
	            UriResourceProperty primitiveProperty
	                                  = (UriResourceProperty) property;
	            propertyNames.add(primitiveProperty.getProperty().getName());
	        }
	        return propertyNames;
	    }
	}

	public Object visitAlias(String aliasName) throws ExpressionVisitException, ODataApplicationException {
		// TODO Auto-generated method stub
		return null;
	}

	public Object visitTypeLiteral(EdmType type) throws ExpressionVisitException, ODataApplicationException {
		// TODO Auto-generated method stub
		return null;
	}

	public Object visitLambdaReference(String variableName) throws ExpressionVisitException, ODataApplicationException {
		// TODO Auto-generated method stub
		return null;
	}

	public Object visitEnum(EdmEnumType type, List enumValues)
			throws ExpressionVisitException, ODataApplicationException {
		// TODO Auto-generated method stub
		return null;
	}

}
