package mvm.rya.shell.command.accumulo.data;

import org.openrdf.model.Statement;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class StatementHandler extends RDFHandlerBase
{
	private final Logger log = LoggerFactory.getLogger(StatementHandler.class);
	private Set<Statement> statements;

	public StatementHandler()
	{
		super();
		statements = new HashSet<>();
	}

	@Override
	public void handleStatement(Statement st)
	{
		if (!statements.add(st))
		{
			log.warn("Attempting to add duplicate statement: {}", st);
		}
	}

	public Set<Statement> getStatements()
	{
		return statements;
	}
}