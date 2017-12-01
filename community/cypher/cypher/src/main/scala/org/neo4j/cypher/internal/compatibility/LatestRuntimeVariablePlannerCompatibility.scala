package org.neo4j.cypher.internal.compatibility

import java.time.Clock

import org.neo4j.cypher._
import org.neo4j.cypher.exceptionHandler.RunSafely
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.procs.ProcedureCallOrSchemaCommandExecutionPlanBuilder
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{ExecutionPlan => ExecutionPlan_v3_4}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{CommunityRuntimeContext => CommunityRuntimeContextV3_4, _}
import org.neo4j.cypher.internal.compatibility.v3_4.{ExceptionTranslatingQueryContext, WrappedMonitors => WrappedMonitorsV3_4}
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.compiler.v3_4.phases.{CompilationContains, LogicalPlanState}
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp._
import org.neo4j.cypher.internal.frontend.v3_4.phases._
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.planner.v3_4.spi.{CostBasedPlannerName, DPPlannerName, IDPPlannerName}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.runtime.{ExplainMode, InternalExecutionResult, NormalMode, ProfileMode}
import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.v3_4.logical.plans.{ExplicitNodeIndexUsage, ExplicitRelationshipIndexUsage, SchemaIndexScanUsage, SchemaIndexSeekUsage}
import org.neo4j.graphdb.Result
import org.neo4j.kernel.api.query.IndexUsage.{explicitIndexUsage, schemaIndexUsage}
import org.neo4j.kernel.api.query.PlannerInfo
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._

trait LatestRuntimeVariablePlannerCompatibility[CONTEXT3_4 <: CommunityRuntimeContextV3_4,
T <: Transformer[CONTEXT3_4, LogicalPlanState, CompilationState],
STATEMENT <: AnyRef] {

  // abstract stuff
  val configV3_4: CypherCompilerConfiguration
  val clock: Clock
  val kernelMonitors: KernelMonitors
  val log: Log
  val planner: CypherPlanner
  val runtime: CypherRuntime
  val updateStrategy: CypherUpdateStrategy
  val runtimeBuilder: RuntimeBuilder[T]
  val contextCreatorV3_4: ContextCreator[CONTEXT3_4]
  val cacheMonitor: AstCacheMonitor[STATEMENT]
  val maybePlannerNameV3_4: Option[CostBasedPlannerName]
  val runSafelyDuringPlanning: RunSafely
  val runSafelyDuringRuntime: RunSafely
  def produceParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer,
                         preParsingNotifications: Set[org.neo4j.graphdb.Notification]): ParsedQuery

  // concrete stuff
  val monitorsV3_4: Monitors = WrappedMonitorsV3_4(kernelMonitors)
  val logger: InfoLogger = new StringInfoLogger(log)
  val cacheAccessor: MonitoringCacheAccessor[STATEMENT, ExecutionPlan_v3_4] = new MonitoringCacheAccessor[STATEMENT, ExecutionPlan_v3_4](cacheMonitor)
  val planCacheFactory = () => new LFUCache[STATEMENT, ExecutionPlan_v3_4](configV3_4.queryCacheSize)
  val maybeRuntimeName: Option[RuntimeName] = runtime match {
    case CypherRuntime.default => None
    case CypherRuntime.interpreted => Some(InterpretedRuntimeName)
    case CypherRuntime.slotted => Some(SlottedRuntimeName)
    case CypherRuntime.morsel => Some(MorselRuntimeName)
    case CypherRuntime.compiled => Some(CompiledRuntimeName)
  }
  implicit lazy val executionMonitor: QueryExecutionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])
  val queryGraphSolverV3_4 = LatestRuntimeVariablePlannerCompatibility.createQueryGraphSolver(
    maybePlannerNameV3_4.getOrElse(CostBasedPlannerName.default),
    monitorsV3_4,
    configV3_4)

  protected def createExecPlan: Transformer[CONTEXT3_4, LogicalPlanState, CompilationState] = {
    ProcedureCallOrSchemaCommandExecutionPlanBuilder andThen
      If((s: CompilationState) => s.maybeExecutionPlan.isEmpty)(
        runtimeBuilder.create(maybeRuntimeName, configV3_4.useErrorsOverWarnings).adds(CompilationContains[ExecutionPlan_v3_4])
      )
  }

  protected def logStalePlanRemovalMonitor(log: InfoLogger) = new AstCacheMonitor[STATEMENT] {
    override def cacheDiscard(key: STATEMENT, userKey: String, secondsSinceReplan: Int) {
      log.info(s"Discarded stale query from the query cache: $userKey")
    }
  }

  protected class ExecutionPlanWrapper(inner: ExecutionPlan_v3_4, preParsingNotifications: Set[org.neo4j.graphdb.Notification], offset: InputPosition)
    extends ExecutionPlan {

    private val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])

    private def queryContext(transactionalContext: TransactionalContextWrapper) = {
      val ctx = new TransactionBoundQueryContext(transactionalContext)(searchMonitor)
      new ExceptionTranslatingQueryContext(ctx)
    }

    def run(transactionalContext: TransactionalContextWrapper, executionMode: CypherExecutionMode,
            params: MapValue): Result = {
      val innerExecutionMode = executionMode match {
        case CypherExecutionMode.explain => ExplainMode
        case CypherExecutionMode.profile => ProfileMode
        case CypherExecutionMode.normal => NormalMode
      }
      runSafelyDuringPlanning {

        val context = queryContext(transactionalContext)

        val innerResult: InternalExecutionResult = inner.run(context, innerExecutionMode, params)
        new ExecutionResult(new ClosingExecutionResult(
          transactionalContext.tc.executingQuery(),
          innerResult.withNotifications(preParsingNotifications.toSeq: _*),
          runSafelyDuringRuntime
        ))
      }
    }

    def isPeriodicCommit: Boolean = inner.isPeriodicCommit

    def isStale(lastCommittedTxId: LastCommittedTxIdProvider, ctx: TransactionalContextWrapper): CacheCheckResult =
      inner.isStale(lastCommittedTxId, TransactionBoundGraphStatistics(ctx.readOperations))

    override val plannerInfo: PlannerInfo = {
      new PlannerInfo(inner.plannerUsed.name, inner.runtimeUsed.name, inner.plannedIndexUsage.map {
        case SchemaIndexSeekUsage(identifier, labelId, label, propertyKeys) => schemaIndexUsage(identifier, labelId, label, propertyKeys: _*)
        case SchemaIndexScanUsage(identifier, labelId, label, propertyKey) => schemaIndexUsage(identifier, labelId, label, propertyKey)
        case ExplicitNodeIndexUsage(identifier, index) => explicitIndexUsage(identifier, "NODE", index)
        case ExplicitRelationshipIndexUsage(identifier, index) => explicitIndexUsage(identifier, "RELATIONSHIP", index)
      }.asJava)
    }
  }


}

object LatestRuntimeVariablePlannerCompatibility {
  def createQueryGraphSolver(n: CostBasedPlannerName, monitors: Monitors,
                             config: CypherCompilerConfiguration): QueryGraphSolver = n match {
    case IDPPlannerName =>
      val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
      val solverConfig = new ConfigurableIDPSolverConfig(
        maxTableSize = config.idpMaxTableSize,
        iterationDurationLimit = config.idpIterationDuration
      )
      val singleComponentPlanner = SingleComponentPlanner(monitor, solverConfig)
      IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)

    case DPPlannerName =>
      val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
      val singleComponentPlanner = SingleComponentPlanner(monitor, DPSolverConfig)
      IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)
  }
}

trait CypherCacheFlushingMonitor[T] {
  def cacheFlushDetected(justBeforeKey: T) {}
}

trait CypherCacheHitMonitor[T] {
  def cacheHit(key: T) {}
  def cacheMiss(key: T) {}
  def cacheDiscard(key: T, userKey: String, secondsSinceReplan: Int) {}
}

trait CypherCacheMonitor[T, E] extends CypherCacheHitMonitor[T] with CypherCacheFlushingMonitor[E]
trait AstCacheMonitor[STATEMENT <: AnyRef] extends CypherCacheMonitor[STATEMENT, CacheAccessor[STATEMENT, ExecutionPlan_v3_4]]

trait InfoLogger {
  def info(message: String)
}

class StringInfoLogger(log: Log) extends InfoLogger {
  def info(message: String) {
    log.info(message)
  }
}
