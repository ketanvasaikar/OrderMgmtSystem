
package com.ordermgmtsystem.oms.util;

import static com.ordermgmtsystem.oms.order.EnhancedOrderConstants.IXM_LOAD_BUILDER;
import static com.ordermgmtsystem.oms.util.PackingResourceUtil.deadStates;
import static java.util.stream.Collectors.toList;

import java.io.*;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.SerializationUtils;
import org.joda.time.DateTimeComparator;
import org.json.JSONException;
import org.json.JSONObject;

import com.ordermgmtsystem.oms.alert.AlertUtil;
import com.ordermgmtsystem.oms.base.OMSUtil;
import com.ordermgmtsystem.oms.base.PartnerUtil;
import com.ordermgmtsystem.oms.cache.AvlKey;
import com.ordermgmtsystem.oms.cache.TemplateKey;
import com.ordermgmtsystem.oms.cache.TransactionCache;
import com.ordermgmtsystem.oms.contract.ContractConstants;
import com.ordermgmtsystem.oms.contract.ContractResult;
import com.ordermgmtsystem.oms.contract.OrderNumberResult;
import com.ordermgmtsystem.oms.contract.dao.ContractTermsDao;
import com.ordermgmtsystem.oms.contract.sourcing.ContractSourcingPolicy;
import com.ordermgmtsystem.oms.contract.sourcing.ContractSourcingPolicyFactory;
import com.ordermgmtsystem.oms.cp.*;
import com.ordermgmtsystem.oms.customization.EDICodePopulator;
import com.ordermgmtsystem.oms.domain.collaboration.AbstractOrderCollaboration;
import com.ordermgmtsystem.oms.domain.impl.DomainFactory;
import com.ordermgmtsystem.oms.domain.validation.Contract.ContractHoldConstants;
import com.ordermgmtsystem.oms.domain.validation.order.DuplicateItemsValidator;
import com.ordermgmtsystem.oms.domain.validation.order.ManageHoldOperations;
import com.ordermgmtsystem.oms.enums.Policy;
import com.ordermgmtsystem.oms.ixm.ordervalidationengine.AbstractOrderValidator;
import com.ordermgmtsystem.oms.ixm.ordervalidationengine.OrderHoldConstants;
import com.ordermgmtsystem.oms.model.*;
import com.ordermgmtsystem.oms.model.BufferDetailMDFs;
import com.ordermgmtsystem.oms.model.ItemMDFs;
import com.ordermgmtsystem.oms.model.dao.OmsContractLineRow;
import com.ordermgmtsystem.oms.model.dao.OmsContractTermsKey;
import com.ordermgmtsystem.oms.model.dao.OmsOrgProcurmntPolicyRow;
import com.ordermgmtsystem.oms.model.enums.*;
import com.ordermgmtsystem.oms.model.enums.OriginEnum;
import com.ordermgmtsystem.oms.mpt.OMSConstants;
import com.ordermgmtsystem.oms.mpt.OMSConstants.Policies;
import com.ordermgmtsystem.oms.mpt.SCCEnhancedOrderConstants;
import com.ordermgmtsystem.oms.mpt.SCCEnhancedOrderPrivateConstants;
import com.ordermgmtsystem.oms.mpt.SCCEnhancedOrderConstants.Actions;
import com.ordermgmtsystem.oms.mpt.SCCEnhancedOrderConstants.States;
import com.ordermgmtsystem.oms.order.EnhancedOrderConstants;
import com.ordermgmtsystem.oms.order.validation.OrderDomainValidator;
import com.ordermgmtsystem.oms.rest.ModelDataServiceUtil;
import com.ordermgmtsystem.oms.rest.OrderRestUtil;
import com.ordermgmtsystem.oms.rest.contract.ContractRestUtil;
import com.ordermgmtsystem.oms.rest.util.ExternalRefUtil;
import com.ordermgmtsystem.oms.server.OmsConstants;
import com.ordermgmtsystem.oms.server.base.order.*;
import com.ordermgmtsystem.oms.service.calendar.BusinessCalendar;
import com.ordermgmtsystem.oms.service.calendar.BusinessCalendarException;
import com.ordermgmtsystem.oms.service.calendar.BusinessCalendarUtil;
import com.ordermgmtsystem.oms.shipment.ShipmentConstants;
import com.ordermgmtsystem.oms.sourcing.SourcingRequest;
import com.ordermgmtsystem.oms.sourcing.SourcingResponse;
import com.ordermgmtsystem.oms.sourcing.avlline.dao.AvlLineSourcingDao;
import com.ordermgmtsystem.oms.sourcing.avlline.service.AvlLineSourcingService;
import com.ordermgmtsystem.oms.util.order.TransMode;
import com.ordermgmtsystem.oms.util.order.TransModeUtil;
import com.ordermgmtsystem.oms.web.pdf.builder.LocalizationHelper;
import com.ordermgmtsystem.platform.common.*;
import com.ordermgmtsystem.platform.common.Enum;
import com.ordermgmtsystem.platform.common.Model;
import com.ordermgmtsystem.platform.common.address.AddressComponentType;
import com.ordermgmtsystem.platform.common.enums.CurrencyCode;
import com.ordermgmtsystem.platform.common.event.PlatformEvent;
import com.ordermgmtsystem.platform.common.event.PlatformEventService;
import com.ordermgmtsystem.platform.common.field.FieldType;
import com.ordermgmtsystem.platform.common.field.impl.CoreFieldType;
import com.ordermgmtsystem.platform.common.impl.Constants;
import com.ordermgmtsystem.platform.common.mpt.PLTConstants.RoleTypes;
import com.ordermgmtsystem.platform.common.uom.UOMService;
import com.ordermgmtsystem.platform.common.uom.UndefinedConversionFactorException;
import com.ordermgmtsystem.platform.common.usercontext.PlatformUserContext;
import com.ordermgmtsystem.platform.common.usercontext.PlatformUserProfile;
import com.ordermgmtsystem.platform.common.usercontext.UserContextService;
import com.ordermgmtsystem.platform.data.model.*;
import com.ordermgmtsystem.platform.data.model.ModelList;
import com.ordermgmtsystem.platform.data.model.impl.ModelLevelType;
import com.ordermgmtsystem.platform.data.model.impl.ModelType;
import com.ordermgmtsystem.platform.data.model.policy.PolicyService;
import com.ordermgmtsystem.platform.data.sql.*;
import com.ordermgmtsystem.platform.data.sql.impl.IDaoRow;
import com.ordermgmtsystem.platform.env.bean.BeanService;
import com.ordermgmtsystem.platform.env.servicelocator.Services;
import com.ordermgmtsystem.platform.grid.TaskState;
import com.ordermgmtsystem.platform.integ.csv.CsvRow;
import com.ordermgmtsystem.platform.integ.csv.CsvTransformException;
import com.ordermgmtsystem.platform.integ.rest.json.FilterJSONObject;
import com.ordermgmtsystem.platform.tools.collections.DefaultingHashMap;
import com.ordermgmtsystem.platform.tools.collections.ListUtil;
import com.ordermgmtsystem.platform.tools.i18n.MessageBundleFactory;
import com.ordermgmtsystem.platform.tools.log.PlatformLogger;
import com.ordermgmtsystem.platform.tools.util.DynamicLoad;
import com.ordermgmtsystem.platform.data.sql.SqlParams;
import com.ordermgmtsystem.platform.data.sql.TransactionSupport;
import com.ordermgmtsystem.platform.workflow.ActionBasedWorkflowContext;
import com.ordermgmtsystem.supplychaincore.lib.FieldUtil;
import com.ordermgmtsystem.supplychaincore.lib.ModelUtil;
import com.ordermgmtsystem.supplychaincore.model.*;
import com.ordermgmtsystem.supplychaincore.model.Note;
import com.ordermgmtsystem.supplychaincore.model.dao.*;
import com.ordermgmtsystem.supplychaincore.model.enums.*;
import com.ordermgmtsystem.supplychaincore.mpt.*;
import com.ordermgmtsystem.supplychaincore.service.HoldService;
import com.ordermgmtsystem.supplychaincore.service.Service;
import com.ordermgmtsystem.supplychaincore.util.MultiModalShipmentStatusUtil;
import com.transcendsys.platform.base.EnumerationException;
import com.transcendsys.platform.base.bo.*;
import com.transcendsys.platform.base.bo.helper.*;
import com.transcendsys.platform.base.context.DvceContext;
import com.transcendsys.platform.base.context.UserContext;
import com.transcendsys.platform.base.dao.*;
import com.transcendsys.platform.base.i18n.LabelUtil;
import com.transcendsys.platform.base.psr.PSRLogger;
import com.transcendsys.platform.base.psr.PSRLoggerEntry;
import com.transcendsys.platform.base.serverenum.EnumerationFactory;
import com.transcendsys.platform.base.serverenum.PdfStringEnum;
import com.transcendsys.platform.base.util.*;
import com.transcendsys.platform.data.model.ModelDiffUtil;
import com.transcendsys.platform.integ.util.ExternalReferenceUtil;
import com.transcendsys.platform.server.RequestScope;
import com.transcendsys.platform.server.cache.manager.*;
import com.transcendsys.platform.server.exception.VCBaseException;
import com.transcendsys.platform.server.model.dma.DirectModelAccess;
import com.transcendsys.platform.server.model.dma.ModelQueryExtensions;
import com.transcendsys.platform.server.report.macro.DerivedFromRoleMacro;
import com.transcendsys.platform.server.vc.helper.EquipmentTypeHelper;
import com.transcendsys.platform.server.workflow.cache.WorkflowCache;
import com.transcendsys.platform.server.workflow.impl.ActionBasedWorkflowContextImpl;
import com.transcendsys.platform.util.locale.LocaleManager;
import com.transcendsys.platform.web.util.StringUtil;

/**
 * Utility methods of Enhanced Order
 */

public class OrderUtil {

  private static final PlatformLogger LOG = PlatformLogger.get(OrderUtil.class);
  private static final UOMService uomService = Services.get(UOMService.class);
  private static final ManageHoldOperations manageHolds = new ManageHoldOperations();
  public static final List<String> nonTransitionalStates = new ArrayList<String>(5);
  public static final List<String> nonCollaborationStates = new ArrayList<String>(5);
  public static final List<String> collaborationStates = new ArrayList<String>(5);
  public static final List<String> newNonTransitionalStates = new ArrayList<String>(5);
  public static final List<String> newNonCollaborationStates = new ArrayList<String>(5);
  public static final List<String> inFullFillmentStates = new ArrayList<String>(6);
  public static final List<String> invalidStatesForCancel = new ArrayList<String>(2);
  public static final List<String> ignoreStates = new ArrayList<String>(2);
  public static final List<String> invalidStatesForClose = new ArrayList<String>(4);
  public static final List<String> BUYER_CHANGE_REQUEST_STATES = new ArrayList<String>(5);
  public static final List<String> VENDOR_CHANGE_REQUEST_STATES = new ArrayList<String>(5);
  public static final List<String> ELIGIBLE_FOR_SHIPPING_STATES = new ArrayList<String>(5);
  public static final List<String> ELIGIBLE_FOR_AUTO_CREATE_SHIPMENT_STATES = new ArrayList<String>(3);
  public static final List<String> ELIGIBLE_STATES_TO_MOVE_IN_OPEN_STATE = new ArrayList<String>(7);
  public static final List<String> ELIGIBLE_STATES_FOR_CANCEL_REMAINING_QTY = new ArrayList<String>(7);
  public static final String GENERATE_LINE_NUMBER = "TempLine";
  private static final String OMS_AUTO_PO_ACK_MODE = PartnerRow.OMS_AUTO_PO_ACK_MODE;
  private static final String OMS_AUTO_CLOSE_ON_RECEIPT = PartnerRow.OMS_AUTO_CLOSE_ON_RECEIPT;
  public static final List<String> DISABLE_ORDER_EDIT_BEYOND_OPEN_STATES = new ArrayList<String>(3);
  private static final String PSR_ID = "OMS-OrderUtil: ";
  public static final String LEAD_TIME = "LeadTime";
  public static final String ORDER_STATE = "OrderState";
  public static final String PRE_ASN_STATE = "PreASNState";
  public static final String RequestScheduleAction_Cancel = "Cancel";
  public static final String ORIGIN_CANCEL_ACTION = "OriginCancelAction";
  public static final String REJECT_VENDOR_CHANGES = "OMS.RejectVendorChanges";
  public static final String REJECT_SHIPPER_CHANGES = "OMS.RejectShipperChanges";
  public static final String REF_TYPE_CONSOLIDATE_PO = "OMS.EnableConsolidatePO Policy";
  public static final String ENABLE_CONSOLIDATE_PO = "OMS.ConsolidatePOFromSalesOrder";
  public static final String CREATE_HOLD_REASON_CODES = "CreateHoldReasonCodes";
  public static final String CLOSED_HOLD_REASON_CODES = "CloseHoldReasonCodes";
  public static final List<String> ORDER_BEFORE_OPEN_STATE = new ArrayList<String>(3);
  public static final List<String> validStateForCancelCollaborationActions = new ArrayList<String>(16);
  public static final List<String> validStateForRSCancelCollaborationActions = new ArrayList<String>(16);
  public static final List<String> VALID_FOR_BUYER_CONFIRM = new ArrayList<String>(3);
  private static final String DELIVERY_DATE_FILTER = "DeliveryDate";
  private static final String HTS_CODE_REGULAR_EXPRESSION = "[0-9]+";
  private static Pattern HTS_CODE_PATTERN = Pattern.compile(HTS_CODE_REGULAR_EXPRESSION);
  public static final List<String> OPW_STATES = ListUtil.create(
    SCCEnhancedOrderConstants.States.OPEN,
    SCCEnhancedOrderConstants.States.VENDOR_CONFIRMED_WITH_CHANGES,
    SCCEnhancedOrderConstants.States.BACKORDERED,
    SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES,
    SCCEnhancedOrderConstants.States.VENDOR_CHANGE_REQUESTED,
    SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED,
    com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.IN_PROMISING,
    SCCEnhancedOrderConstants.States.SELLER_HOLD,
    SCCEnhancedOrderConstants.States.VENDOR_REJECTED,
    SCCEnhancedOrderConstants.States.IN_FULFILLMENT,
    SCCEnhancedOrderConstants.States.PARTIALLY_SHIPPED,
    SCCEnhancedOrderConstants.States.IN_TRANSIT,
    SCCEnhancedOrderConstants.States.PARTIALLY_RECEIVED,
    SCCEnhancedOrderConstants.States.RECEIVED);
  public static final List<String> OPW_RESTRICTED_STATES = Arrays.asList(
    SCCEnhancedOrderConstants.States.VENDOR_CHANGE_REQUESTED,
    SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED,
    com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.IN_PROMISING,
    SCCEnhancedOrderConstants.States.VENDOR_CONFIRMED_WITH_CHANGES,
    SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES,
    SCCEnhancedOrderConstants.States.BACKORDERED);
  public static final List<String> validLineLevelActionsForCancelRemainingQty = Arrays.asList(
    SCCEnhancedOrderPrivateConstants.Actions.ACCEPT_VENDOR_CHANGE_LINE,
    SCCEnhancedOrderPrivateConstants.Actions.REJECT_VENDOR_CHANGE_LINE,
    SCCEnhancedOrderPrivateConstants.Actions.CANCEL_LINE_REMAINING_QUANTITY);
  
  public static final List<String> validActionForBuyerCollabAndBuyerCancelCollab = new ArrayList<String>(16);
  
  public static final List<String> validChildLevelCollaborationActions = new ArrayList<String>(16);
  
  public static final List<String> vendorCollabActionsForInteg = Arrays.asList(
    SCCEnhancedOrderPrivateConstants.Actions.PROMISE_SCHEDULES,
    SCCEnhancedOrderPrivateConstants.Actions.VENDOR_CONFIRM_SCHEDULES,
    SCCEnhancedOrderPrivateConstants.Actions.SHIPPER_CONFIRM_SCHEDULES,
    SCCEnhancedOrderPrivateConstants.Actions.VENDOR_CHANGE_REQUEST_BY_DS,
    SCCEnhancedOrderPrivateConstants.Actions.SHIPPER_CHANGE_REQUEST_BY_DS);
  
  public static final List<String>  buyerCollabActionsForInteg = Arrays.asList(
    SCCEnhancedOrderPrivateConstants.Actions.CONSIGNEE_CONFIRM_SCHEDULES,
    SCCEnhancedOrderPrivateConstants.Actions.BUYER_CONFIRM_SCHEDULES,
    SCCEnhancedOrderPrivateConstants.Actions.CONSIGNEE_CHANGE_REQUEST_SCHEDULES,
    SCCEnhancedOrderPrivateConstants.Actions.BUYER_CHANGE_REQUEST_SCHEDULES);
  
  static {
    nonTransitionalStates.add(States.DELETED);
    nonTransitionalStates.add(States.CLOSED);
    nonTransitionalStates.add(States.CANCELLED);
    nonTransitionalStates.add(States.BACKORDERED);
    nonTransitionalStates.add(States.VENDOR_REJECTED);
    nonTransitionalStates.add(States.CONVERTED);

    invalidStatesForClose.add(States.DELETED);
    invalidStatesForClose.add(States.CLOSED);
    invalidStatesForClose.add(States.CANCELLED);
    invalidStatesForClose.add(States.VENDOR_REJECTED);
    invalidStatesForClose.add(States.CONVERTED);
    
    nonCollaborationStates.add(States.DELETED);
    nonCollaborationStates.add(States.CLOSED);
    nonCollaborationStates.add(States.CANCELLED);
    nonCollaborationStates.add(States.VENDOR_REJECTED);
    nonCollaborationStates.add(States.CONVERTED);
    
    newNonTransitionalStates.add(States.DELETED);
    newNonTransitionalStates.add(States.CLOSED);
    newNonTransitionalStates.add(States.CANCELLED);
    newNonTransitionalStates.add(States.BACKORDERED);
    newNonTransitionalStates.add(States.CONVERTED);

    newNonCollaborationStates.add(States.DELETED);
    newNonCollaborationStates.add(States.CLOSED);
    newNonCollaborationStates.add(States.CANCELLED);
    newNonCollaborationStates.add(States.CONVERTED);

    inFullFillmentStates.add(States.IN_FULFILLMENT);
    inFullFillmentStates.add(States.IN_TRANSIT);
    inFullFillmentStates.add(States.PARTIALLY_SHIPPED);
    inFullFillmentStates.add(States.PARTIALLY_RECEIVED);
    inFullFillmentStates.add(States.RECEIVED);
    inFullFillmentStates.add(States.CLOSED);

    invalidStatesForCancel.add(States.CLOSED);
    invalidStatesForCancel.add(States.DELETED);

    ignoreStates.add(States.DELETED);
    ignoreStates.add(States.CANCELLED);
    ignoreStates.add(States.CONVERTED);
    
    collaborationStates.add(SCCEnhancedOrderConstants.States.VENDOR_CHANGE_REQUESTED);
    collaborationStates.add(SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED);
    collaborationStates.add(com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.IN_PROMISING);
    collaborationStates.add(SCCEnhancedOrderConstants.States.VENDOR_CONFIRMED_WITH_CHANGES);
    collaborationStates.add(SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES);

    BUYER_CHANGE_REQUEST_STATES.add(States.OPEN);
    BUYER_CHANGE_REQUEST_STATES.add(States.IN_FULFILLMENT);
    BUYER_CHANGE_REQUEST_STATES.add(States.IN_TRANSIT);
    BUYER_CHANGE_REQUEST_STATES.add(States.PARTIALLY_SHIPPED);
    BUYER_CHANGE_REQUEST_STATES.add(States.PARTIALLY_RECEIVED);
    BUYER_CHANGE_REQUEST_STATES.add(States.RECEIVED);

    VENDOR_CHANGE_REQUEST_STATES.add(States.OPEN);
    VENDOR_CHANGE_REQUEST_STATES.add(States.IN_FULFILLMENT);
    VENDOR_CHANGE_REQUEST_STATES.add(States.IN_TRANSIT);
    VENDOR_CHANGE_REQUEST_STATES.add(States.PARTIALLY_SHIPPED);
    VENDOR_CHANGE_REQUEST_STATES.add(States.PARTIALLY_RECEIVED);
    VENDOR_CHANGE_REQUEST_STATES.add(States.RECEIVED);

    ELIGIBLE_FOR_SHIPPING_STATES.add(States.OPEN);
    ELIGIBLE_FOR_SHIPPING_STATES.add(States.IN_FULFILLMENT);
    ELIGIBLE_FOR_SHIPPING_STATES.add(States.PARTIALLY_SHIPPED);
    ELIGIBLE_FOR_SHIPPING_STATES.add(States.PARTIALLY_RECEIVED);

    ELIGIBLE_STATES_TO_MOVE_IN_OPEN_STATE.add(States.NEW);
    ELIGIBLE_STATES_TO_MOVE_IN_OPEN_STATE.add(States.VENDOR_CONFIRMED_WITH_CHANGES);
    ELIGIBLE_STATES_TO_MOVE_IN_OPEN_STATE.add(States.BUYER_CONFIRMED_WITH_CHANGES);
    ELIGIBLE_STATES_TO_MOVE_IN_OPEN_STATE.add(States.BUYER_CHANGE_REQUESTED);
    ELIGIBLE_STATES_TO_MOVE_IN_OPEN_STATE.add(States.VENDOR_CHANGE_REQUESTED);
    ELIGIBLE_STATES_TO_MOVE_IN_OPEN_STATE.add(States.SELLER_HOLD);
    ELIGIBLE_STATES_TO_MOVE_IN_OPEN_STATE.add(
      com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.IN_PROMISING);
    
    ELIGIBLE_STATES_FOR_CANCEL_REMAINING_QTY.add(States.PARTIALLY_SHIPPED);
    ELIGIBLE_STATES_FOR_CANCEL_REMAINING_QTY.add(States.PARTIALLY_RECEIVED);
    
    DISABLE_ORDER_EDIT_BEYOND_OPEN_STATES.add(
      com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.DRAFT);
    DISABLE_ORDER_EDIT_BEYOND_OPEN_STATES.add(
      com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.AWAITING_APPROVAL);
    DISABLE_ORDER_EDIT_BEYOND_OPEN_STATES.add(com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.NEW);
    DISABLE_ORDER_EDIT_BEYOND_OPEN_STATES.add(
      com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.VENDOR_CONFIRMED_WITH_CHANGES);
    DISABLE_ORDER_EDIT_BEYOND_OPEN_STATES.add(
      com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES);
    DISABLE_ORDER_EDIT_BEYOND_OPEN_STATES.add(
      com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED);
    DISABLE_ORDER_EDIT_BEYOND_OPEN_STATES.add(
      com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.VENDOR_CHANGE_REQUESTED);
    DISABLE_ORDER_EDIT_BEYOND_OPEN_STATES.add(
      com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.BUYER_HOLD);
    DISABLE_ORDER_EDIT_BEYOND_OPEN_STATES.add(
      com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.SELLER_HOLD);
    DISABLE_ORDER_EDIT_BEYOND_OPEN_STATES.add(
      com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.IN_PROMISING);

    ELIGIBLE_FOR_AUTO_CREATE_SHIPMENT_STATES.add(AutoCreateShipmentEnum.AWAITING_APPROVAL.stringValue());
    ELIGIBLE_FOR_AUTO_CREATE_SHIPMENT_STATES.add(AutoCreateShipmentEnum.NEW.stringValue());
    ELIGIBLE_FOR_AUTO_CREATE_SHIPMENT_STATES.add(AutoCreateShipmentEnum.OPEN.stringValue());
    ELIGIBLE_FOR_AUTO_CREATE_SHIPMENT_STATES.add(AutoCreateShipmentEnum.IN_PROMISING.stringValue());

    ORDER_BEFORE_OPEN_STATE.add(com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.DRAFT);
    ORDER_BEFORE_OPEN_STATE.add(com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.AWAITING_APPROVAL);
    ORDER_BEFORE_OPEN_STATE.add(com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.NEW);
    
    
    validStateForCancelCollaborationActions.add(States.NEW);
    validStateForCancelCollaborationActions.add(States.VENDOR_CONFIRMED_WITH_CHANGES);
    validStateForCancelCollaborationActions.add(States.BACKORDERED);
    validStateForCancelCollaborationActions.add(States.BUYER_CONFIRMED_WITH_CHANGES);
    validStateForCancelCollaborationActions.add("In Promising");
    validStateForCancelCollaborationActions.add(States.OPEN);
    validStateForCancelCollaborationActions.add(States.BUYER_CHANGE_REQUESTED);
    validStateForCancelCollaborationActions.add(States.VENDOR_CHANGE_REQUESTED);
    validStateForCancelCollaborationActions.add(States.BUYER_HOLD);
    validStateForCancelCollaborationActions.add(States.SELLER_HOLD);
    validStateForCancelCollaborationActions.add(States.PARTIALLY_RECEIVED);
    validStateForCancelCollaborationActions.add(States.IN_FULFILLMENT);
    validStateForCancelCollaborationActions.add(States.PARTIALLY_SHIPPED);
    
    validStateForRSCancelCollaborationActions.add(States.NEW);
    validStateForRSCancelCollaborationActions.add(States.VENDOR_CONFIRMED_WITH_CHANGES);
    validStateForRSCancelCollaborationActions.add(States.BACKORDERED);
    validStateForRSCancelCollaborationActions.add(States.BUYER_CONFIRMED_WITH_CHANGES);
    validStateForRSCancelCollaborationActions.add("In Promising");
    validStateForRSCancelCollaborationActions.add(States.OPEN);
    validStateForRSCancelCollaborationActions.add(States.BUYER_CHANGE_REQUESTED);
    validStateForRSCancelCollaborationActions.add(States.VENDOR_CHANGE_REQUESTED);
    validStateForRSCancelCollaborationActions.add(States.BUYER_HOLD);
    validStateForRSCancelCollaborationActions.add(States.SELLER_HOLD);
    
    validActionForBuyerCollabAndBuyerCancelCollab.add(SCCEnhancedOrderConstants.Actions.APPROVE_CANCEL_LINE_REQUEST);
    validActionForBuyerCollabAndBuyerCancelCollab.add(SCCEnhancedOrderConstants.Actions.REJECT_CANCEL_LINE_REQUEST);
    validActionForBuyerCollabAndBuyerCancelCollab.add(SCCEnhancedOrderConstants.Actions.APPROVE_CANCEL_REQUEST_SCHEDULE_REQUEST); 
    validActionForBuyerCollabAndBuyerCancelCollab.add(SCCEnhancedOrderConstants.Actions.REJECT_CANCEL_REQUEST_SCHEDULE_REQUEST);
    validActionForBuyerCollabAndBuyerCancelCollab.add("OMS.AcceptVendorChangeLine");
    validActionForBuyerCollabAndBuyerCancelCollab.add("OMS.AcceptVendorRSChanges");
    validActionForBuyerCollabAndBuyerCancelCollab.add("OMS.RejectVendorChangeLine");
    validActionForBuyerCollabAndBuyerCancelCollab.add("OMS.RejectVendorRSChanges");
    validActionForBuyerCollabAndBuyerCancelCollab.add("OMS.AcceptShipperChangeLine");
    validActionForBuyerCollabAndBuyerCancelCollab.add("OMS.AcceptShipperRSChange");
    validActionForBuyerCollabAndBuyerCancelCollab.add("OMS.RejectShipperChangeLine");
    validActionForBuyerCollabAndBuyerCancelCollab.add("OMS.RejectShipperRSChange");
    
    validChildLevelCollaborationActions.add(SCCEnhancedOrderConstants.Actions.BUYER_CONFIRM_SCHEDULES);
    validChildLevelCollaborationActions.add(SCCEnhancedOrderPrivateConstants.Actions.BUYER_CHANGE_REQUEST_SCHEDULES);
    validChildLevelCollaborationActions.add(SCCEnhancedOrderPrivateConstants.Actions.CONSIGNEE_CONFIRM_SCHEDULES);
    validChildLevelCollaborationActions.add(SCCEnhancedOrderPrivateConstants.Actions.CONSIGNEE_CHANGE_REQUEST_SCHEDULES);
    
    VALID_FOR_BUYER_CONFIRM.add(States.VENDOR_CHANGE_REQUESTED);
    VALID_FOR_BUYER_CONFIRM.add(States.VENDOR_CONFIRMED_WITH_CHANGES);
    VALID_FOR_BUYER_CONFIRM.add(States.BUYER_CONFIRMED_WITH_CHANGES);
    
  }

  public static DeliverySchedule findMatchingDeliverySchedule(
    EnhancedOrder currentOrder,
    DeliverySchedule inputSchedule) {
    if (Objects.isNull(currentOrder))
      return null;

    for (OrderLine currentLine : currentOrder.getOrderLines()) {
      for (RequestSchedule reqSchedule : currentLine.getRequestSchedules()) {
        for (DeliverySchedule delSchedule : reqSchedule.getDeliverySchedules()) {
          if (delSchedule.equals(inputSchedule)
            || !FieldUtil.isDifferent(delSchedule.getSysId(), inputSchedule.getSysId())) {
            return delSchedule;
          }
        }
      }
    }
    return null;
  }
  
  public static Boolean isDeliveryDateDefault() {
    String localValue = ExternalReferenceUtil.getLocalValue("OrderDefaultDate", "DefaultDate");
    if(!FieldUtil.isNull(localValue) && localValue.equalsIgnoreCase("DeliveryDate")) {
      return true;
    } else {
      return false;
    }   
  }
  
  public static Boolean isSetTimeZoneForRespectiveDates() {
	  String blockTimeZoneSetting = ExternalReferenceUtil.getLocalValue(EnhancedOrderConstants.SET_TZ_TO_DATES, EnhancedOrderConstants.SET_TZ_TO_DATES);
	  if(Objects.nonNull(blockTimeZoneSetting))
		  return !Boolean.valueOf(blockTimeZoneSetting); 
	  return true;
  }
  
  public static EnhancedOrder findMatchingEnhancedOrder(
		    List<EnhancedOrder> currentOrders,
		    EnhancedOrder inputOrder) {
	  if (Objects.isNull(inputOrder) || currentOrders.isEmpty())
		  return null;

	  for (EnhancedOrder currentOrder : currentOrders) {
		  if(currentOrder.equals(inputOrder)) {
			  return currentOrder;
		  }
	  }
	  return null;
  }

  public static RequestSchedule findMatchingRequestSchedule(EnhancedOrder currentOrder, RequestSchedule inputSchedule) {
    if (Objects.isNull(currentOrder))
      return null;

    for (OrderLine currentLine : currentOrder.getOrderLines()) {
      for (RequestSchedule reqSchedule : currentLine.getRequestSchedules()) {
        if (reqSchedule.equals(inputSchedule)) {
          return reqSchedule;
        }
      }
    }
    return null;
  }

  public static OrderLine findMatchingOrderLine(EnhancedOrder currentOrder, OrderLine orderLine) {
    if (Objects.isNull(currentOrder))
      return null;
    for (OrderLine currentLine : currentOrder.getOrderLines()) {
      if (currentLine.equals(orderLine)) {
        return currentLine;
      }
    }
    return null;
  }
  
  public static boolean isBuyerContext(PlatformUserContext ctx, EnhancedOrder order) {
    return isBuyerContext(ctx, order, null);
  }

  public static boolean isBuyerContext(PlatformUserContext ctx, EnhancedOrder order, Boolean isPartOfHierarchyOrg) {
	  boolean returnFlag = false;
	  boolean dualRole = false;
	  Long userOrg = ctx.getRoleOrganizationId();
	  if (!(ctx.isDerivedFrom(SCCConstants.RoleTypes.BUYER_PLANNER)
			  || ctx.isDerivedFrom(SCCConstants.RoleTypes.WAREHOUSE_CLERK)
			  || ctx.isDerivedFrom(SCCConstants.RoleTypes.BUYER_FINANCIALS_MANAGER))) {
		  returnFlag = false;
	  }
	  if (isPartOfHierarchyOrg == null) {
		  isPartOfHierarchyOrg = OrderUtil.isHierarchyContext(ctx, order);
	  }
	  if (((ctx.isDerivedFrom(SCCConstants.RoleTypes.BUYER_PLANNER)
			  || ctx.isDerivedFrom(SCCConstants.RoleTypes.WAREHOUSE_CLERK)
			  || ctx.isDerivedFrom(SCCConstants.RoleTypes.BUYER_FINANCIALS_MANAGER)) && ctx.isDerivedFrom(SCCConstants.RoleTypes.VENDOR_CSR))) {
		  dualRole = true;
	  }
	  if (null != userOrg && !FieldUtil.isNull(order.getSysBuyingOrgId())
			  && (userOrg.equals(order.getSysBuyingOrgId()) || userOrg.equals(order.getSysCustOfBuyerOrgId())
			    || (userOrg.equals(order.getSysOrderMgmtOrgId()) && order.isIsVMI()))) {
		  returnFlag = true;
	  }
	  else if (!order.isIsVMI() && null != userOrg && OrderRestUtil.checkManagingPartnerForOrder(order, ctx, isPartOfHierarchyOrg)
			  && (!FieldUtil.isNull(order.getSysSellingOrgId()) && !userOrg.equals(order.getSysSellingOrgId()))
			  && (((FieldUtil.isNull(order.getSysFulfillmentOrgId()) || ( !FieldUtil.isNull(order.getSysFulfillmentOrgId()) && !userOrg.equals(order.getSysFulfillmentOrgId())))|| 
					  (!FieldUtil.isNull(order.getSysFulfillmentOrgId()) && userOrg.equals(order.getSysFulfillmentOrgId())
							  && !FieldUtil.isNull(order.getSysOrderMgmtOrgId()) && userOrg.equals(order.getSysOrderMgmtOrgId())
							  && OrderTypeEnum.SALES_ORDER.stringValue().equals(order.getOrderType()))
					  )&& (!dualRole && !ctx.isDerivedFrom(SCCConstants.RoleTypes.VENDOR_CSR)))) {
		  returnFlag = true; // this will be true when OMO/Buying agents is buyer and not Seller
	  }
	  else if (!order.isIsVMI() && null != userOrg && OrderRestUtil.checkManagingPartnerForOrder(order, ctx, isPartOfHierarchyOrg)
			  && (!FieldUtil.isNull(order.getSysSellingOrgId()) && !userOrg.equals(order.getSysSellingOrgId()))
			  && (dualRole)) {
		  returnFlag = true; // this will be true when OMO/Buying agents is buyer and not Seller
	  }
	  else if (ctx.isDerivedFrom(SCCConstants.RoleTypes.BUYER_PLANNER) && !order.isIsVMI()
			  && OrderRestUtil.checkManagingPartnerForOrder(order, ctx, isPartOfHierarchyOrg)
			  && (OrderTypeEnum.PURCHASE_ORDER.stringValue().equals(order.getOrderType())
					  || OrderTypeEnum.DEPLOYMENT_ORDER.stringValue().equals(order.getOrderType()))) {
		  returnFlag = true;
	  }
	  else if (null != userOrg
			  && (isShipToOrgMaching(order, userOrg) || OrderRestUtil.checkManagingPartnerForOrder(order, ctx, isPartOfHierarchyOrg))
			  && !FieldUtil.isNull(order.getSysSellingOrgId()) && !userOrg.equals(order.getSysSellingOrgId())
			  && (!FieldUtil.isNull(order.getSysFulfillmentOrgId()) && !userOrg.equals(order.getSysFulfillmentOrgId()) && (!dualRole && !ctx.isDerivedFrom(SCCConstants.RoleTypes.VENDOR_CSR)))) {
		  returnFlag = true; // this will be true when user is from ship to org but not from seller org
	  }
	  else if (null != userOrg
			  && (isShipToOrgMaching(order, userOrg) || OrderRestUtil.checkManagingPartnerForOrder(order, ctx, isPartOfHierarchyOrg))
			  && !FieldUtil.isNull(order.getSysSellingOrgId()) && !userOrg.equals(order.getSysSellingOrgId())
			  &&  dualRole) {
		  returnFlag = true; // this will be true when user is from ship to org but not from seller org
	  }
	  else if (order.getOrderType().equals(OrderTypeEnum.RETURN_ORDER.toString())
			  && !FieldUtil.isNull(order.getSysBuyingOrgId())
			  && (order.getSysBuyingOrgId().equals(userOrg) || order.getSysOwningOrgId().equals(userOrg))) {
		  returnFlag = true;
	  }
	  return returnFlag;
  }

  public static boolean isOrchestratorWithDualContext(PlatformUserContext ctx, EnhancedOrder order) {
    Boolean isOrchestrator = ctx.isDerivedFrom(SCCPrivateConstants.RoleTypes.ORCHESTRATOR);
    if (isOrchestrator && !FieldUtil.isNull(order.getSysOrderMgmtOrgId())
      && ctx.getRoleOrganizationId().equals(order.getSysOrderMgmtOrgId())
      && !FieldUtil.isNull(order.getSysFulfillmentOrgId())
      && ctx.getRoleOrganizationId().equals(order.getSysFulfillmentOrgId())) {
      return true;
    }
    return false;
  }

  private static boolean isShipToOrgMaching(EnhancedOrder order, Long orgId) {
    for (OrderLine line : order.getOrderLines()) {
      for (RequestSchedule reqSchedule : line.getRequestSchedules()) {
        if (!FieldUtil.isNull(reqSchedule.getSysRsShipToOrgId()) && reqSchedule.getSysRsShipToOrgId().equals(orgId))
          return true;
      }
    }
    return false;
  }

  private static boolean isShipFromOrgMatching(EnhancedOrder order, Long orgId) {
    for (OrderLine line : order.getOrderLines()) {
      for (RequestSchedule reqSchedule : line.getRequestSchedules()) {
        for (DeliverySchedule ds : reqSchedule.getDeliverySchedules()) {
          if (!FieldUtil.isNull(ds.getSysDsShipFromOrgId()) && ds.getSysDsShipFromOrgId().equals(orgId))
            return true;
        }
      }
    }
    return false;
  }
  
  public static boolean isSellerContext(PlatformUserContext ctx, EnhancedOrder order, Boolean isEnterpriseRole) {
	  return isSellerContext(ctx, order, isEnterpriseRole, null);
  }

  public static boolean isSellerContext(PlatformUserContext ctx, EnhancedOrder order, Boolean isEnterpriseRole,Boolean isPartOfHierarchyOrg) {
    boolean returnFlag = false;

    if(isPartOfHierarchyOrg == null) {
    	isPartOfHierarchyOrg = OrderUtil.isHierarchyContext(ctx, order);	
    }
    Long userOrg = ctx.getRoleOrganizationId();
    if (!(ctx.isDerivedFrom(SCCConstants.RoleTypes.VENDOR_CSR)
      || ctx.isDerivedFrom(SCCConstants.RoleTypes.VENDOR_FINANCIALS_MANAGER))) {
      if (null != userOrg && !FieldUtil.isNull(order.getSysSellingOrgId())
        && order.getOrderType().equals(OrderTypeEnum.SALES_ORDER.toString())
        && userOrg.equals(order.getSysSellingOrgId())) {
        returnFlag = true;
      }
      else if (order.getOrderType().equals(OrderTypeEnum.DEPLOYMENT_ORDER.toString())) {
        if (isShipFromOrgMatching(order, userOrg)
          || (null != userOrg && OrderRestUtil.check3PLPartnerForOrder(order, ctx, isPartOfHierarchyOrg))) {
          returnFlag = true;
        }
      }
    }
    else if (null != userOrg && !FieldUtil.isNull(order.getSysSellingOrgId())
    		&& userOrg.equals(order.getSysSellingOrgId())) {
    	returnFlag = true;
    }
    else if (null != userOrg && !FieldUtil.isNull(order.getSysBuyingOrgId())
      && ((userOrg.equals(order.getSysBuyingOrgId()) || userOrg.equals(order.getSysCustOfBuyerOrgId()))
        && !isEnterpriseRole)) {
      returnFlag = false;
    }
    else if (order.isIsVMI()
      && (null != userOrg && !FieldUtil.isNull(order.getSysOrderMgmtOrgId())
        && userOrg.equals(order.getSysOrderMgmtOrgId()))
      && (!FieldUtil.isNull(order.getSysBuyingOrgId()) && !userOrg.equals(order.getSysBuyingOrgId()))) {
      returnFlag = true; // this will be true when OMO is not buyer and not Seller
    }
    else if (null != userOrg
      && (isShipFromOrgMatching(order, userOrg) || OrderRestUtil.check3PLPartnerForOrder(order, ctx, isPartOfHierarchyOrg))
      && (!userOrg.equals(order.getSysBuyingOrgId()) || isEnterpriseRole)) {
      returnFlag = true;
    }
    if (ctx.isDerivedFrom(SCCConstants.RoleTypes.VENDOR_CSR)
      && order.getOrderType().equals(OrderTypeEnum.DEPLOYMENT_ORDER.toString())) {
      returnFlag = true;
    }
    else if (order.getOrderType().equals(OrderTypeEnum.RETURN_ORDER.toString())
      && !FieldUtil.isNull(order.getSysSellingOrgId())
      && (order.getSysSellingOrgId().equals(userOrg) || order.getSysOwningOrgId().equals(userOrg)))
      returnFlag = true;
    return returnFlag;
  }

  /**
   * TODO complete method documentation
   *
   * @param platformUserContext
   * @param enhancedOrder
   */
  public static boolean isHierarchyContext(PlatformUserContext platformUserContext, EnhancedOrder enhancedOrder) {
	  if(!FieldUtil.isNull(platformUserContext.getRoleOrganizationId())) {
		  boolean isSalesOrder = enhancedOrder.getOrderType().equals(OrderTypeEnum.SALES_ORDER.toString()) ? true : false;
		  Long orgId = isSalesOrder ? enhancedOrder.getSysSellingOrgId() : enhancedOrder.getSysBuyingOrgId();
		  if(!FieldUtil.isNull(orgId)){
			  OrgHierarchyService orgHierarchyService = Services.get(OrgHierarchyService.class);
			  RoleRow role = RoleCacheManager.getInstance().getRole(platformUserContext.getRoleId(), (DvceContext) platformUserContext);
			  if(Objects.nonNull(role) && !FieldUtil.isNull(role.getSysOrgHierarchyForReadId())) {
				  Set<Long> childOrgIds = orgHierarchyService.getChildrenRecursively(platformUserContext.getRoleOrganizationId(), role.getSysOrgHierarchyForReadId());
				  if(Objects.nonNull(childOrgIds) && !childOrgIds.isEmpty()) {
					  if (childOrgIds.contains(orgId)) {
						  if(isSalesOrder) {
							  if(platformUserContext.isDerivedFrom(SCCConstants.RoleTypes.VENDOR_CSR)) {
								  return true;
							  }
						  } else if(platformUserContext.isDerivedFrom(SCCConstants.RoleTypes.BUYER_PLANNER)) {
							  return true;
						  }
					  }
				  }
			  }
		  }
    }
    else if (!FieldUtil.isNull(platformUserContext.getRoleEnterpriseId())
      && !FieldUtil.isNull(platformUserContext.getRoleOrganizationId())) {
      SqlParams sqlParams = new SqlParams();
		  sqlParams.setLongValue("MY_ENT_ID", platformUserContext.getRoleEnterpriseId());
		  sqlParams.setLongValue("MY_ROLE_ID", platformUserContext.getRoleId());
		  if(!FieldUtil.isNull(platformUserContext.getRoleOrganizationId())){
			  sqlParams.setLongValue("MY_ORG_ID", platformUserContext.getRoleOrganizationId());
		  } else {
			  return true;
		  }
		  sqlParams.setLongValue("MY_VC_ID", platformUserContext.getValueChainId());
		  SqlResult sqlResult = ModelDataServiceUtil.executeAutocompleteSqlResult(
				  "PLT.OrgHierarchySqls",
				  "MyAndChildOrgIds",
				  sqlParams,
				  platformUserContext);
		  if (Objects.nonNull(sqlResult) && Objects.nonNull(sqlResult.getRows()) && !sqlResult.getRows().isEmpty()
				  && !FieldUtil.isNull(enhancedOrder.getSysBuyingOrgId())) {
			  for (SqlRow row : sqlResult.getRows()) {
				  if (!row.isNull("SYS_CHILD_ORG_ID")) {
					  if (enhancedOrder.getSysBuyingOrgId().equals(row.getLongValue("SYS_CHILD_ORG_ID"))
							  && platformUserContext.isDerivedFrom(SCCConstants.RoleTypes.BUYER_PLANNER)) {
						  return true;
					  }
				  }
			  }
		  }
	  }
    return false;
  }
  
  public static boolean isReadOnlyRoleType( EnhancedOrder order, PlatformUserContext ctx) {
    Boolean isEnterpriseRole = ctx.isDerivedFrom(RoleTypes.ENTERPRISE_ADMIN);
    Boolean isEnterpriseLevelRole = !FieldUtil.isNull(ctx.getRoleEnterpriseId()) && FieldUtil.isNull(ctx.getRoleOrganizationId());
    if(isEnterpriseRole || isEnterpriseLevelRole) {
      List<Long> orgIds = new ArrayList<Long>();
      orgIds.add(order.getSysBuyingOrgId());
      orgIds.add(order.getSysSellingOrgId());
      orgIds.add(order.getSysFulfillmentOrgId());
      orgIds.add(order.getSysOrderMgmtOrgId());
      orgIds.add(order.getSysSellingAgent1Id());
      orgIds.add(order.getSysSellingAgent2Id());
      orgIds.add(order.getSysSellingAgent3Id());
      orgIds.add(order.getSysSellingAgent4Id());
      orgIds.add(order.getSysBuyingAgent1Id());
      orgIds.add(order.getSysBuyingAgent2Id());
      orgIds.add(order.getSysBuyingAgent3Id());
      orgIds.add(order.getSysBuyingAgent4Id());
      orgIds = orgIds.stream().filter(org->!FieldUtil.isNull(org)).collect(Collectors.toList());
      if(!orgIds.isEmpty() && getEnterpriseIds(orgIds, (DvceContext) ctx).contains(ctx.getRoleEnterpriseId())) {
        return true;
      }
    }
      return false;
  }
  
  public static Pair<Pair<Boolean, Boolean>, Pair<Boolean, Boolean>> determineOrderContextForReadOnlyRole(
    EnhancedOrder order,
    PlatformUserContext ctx) {
      Long roleEntId = ctx.getRoleEnterpriseId();
      Pair<Pair<Boolean, Boolean>, Pair<Boolean, Boolean>> contextPair = null;
      List<Long> buyerRoleOrgs = new ArrayList<Long>();
      buyerRoleOrgs.add(order.getSysBuyingOrgId());
      buyerRoleOrgs.add(order.getSysOrderMgmtOrgId());
      buyerRoleOrgs.add(order.getSysBuyingAgent1Id());
      buyerRoleOrgs.add(order.getSysBuyingAgent2Id());
      buyerRoleOrgs.add(order.getSysBuyingAgent3Id());
      buyerRoleOrgs.add(order.getSysBuyingAgent4Id());
      List<Long> sellerRoleOrgs = new ArrayList<Long>();
      sellerRoleOrgs.add(order.getSysSellingOrgId());
      sellerRoleOrgs.add(order.getSysFulfillmentOrgId());
      sellerRoleOrgs.add(order.getSysSellingAgent1Id());
      sellerRoleOrgs.add(order.getSysSellingAgent2Id());
      sellerRoleOrgs.add(order.getSysSellingAgent3Id());
      sellerRoleOrgs.add(order.getSysSellingAgent4Id());
      List<Long> buyerEntIds = getEnterpriseIds(buyerRoleOrgs, (DvceContext) ctx);
      List<Long> sellerEntIds = getEnterpriseIds(sellerRoleOrgs, (DvceContext) ctx);
      String orderType = OrderRestUtil.getOrderType(order);
      if (orderType.equals(com.ordermgmtsystem.oms.rest.EnhancedOrderConstants.ORDER_TYPE_PURCHASE_ORDER)
        || orderType.equals(com.ordermgmtsystem.oms.rest.EnhancedOrderConstants.ORDER_TYPE_DEPLOYMENT_ORDER)
        || orderType.equals(com.ordermgmtsystem.oms.rest.EnhancedOrderConstants.ORDER_TYPE_PO_RELEASE_ORDER)
        || orderType.equals(com.ordermgmtsystem.oms.rest.EnhancedOrderConstants.ORDER_TYPE_SPOT_ORDER)
        || orderType.equals(com.ordermgmtsystem.oms.rest.EnhancedOrderConstants.ORDER_TYPE_RETURN_ORDER)
        || orderType.equals(com.ordermgmtsystem.oms.rest.EnhancedOrderConstants.ORDER_TYPE_VMI_ORDER)
        || orderType.equals(com.ordermgmtsystem.oms.rest.EnhancedOrderConstants.ORDER_TYPE_VMI_PO_RELEASE_ORDER)
        || orderType.equals(com.ordermgmtsystem.oms.rest.EnhancedOrderConstants.ORDER_TYPE_SALES_ORDER)) {
        
        if (buyerEntIds.contains(roleEntId) && !sellerEntIds.contains(roleEntId)) {
          contextPair = new Pair<>(new Pair<>(true, false), new Pair<>(false, false));
        }
        if (!buyerEntIds.contains(roleEntId) && sellerEntIds.contains(roleEntId)) {
          contextPair = new Pair<>(new Pair<>(false, true), new Pair<>(false, false));
        }
        if (buyerEntIds.contains(roleEntId) && sellerEntIds.contains(roleEntId)) {
          if (orderType.equals(com.ordermgmtsystem.oms.rest.EnhancedOrderConstants.ORDER_TYPE_SALES_ORDER)) {
            contextPair = new Pair<>(new Pair<>(false, true), new Pair<>(true, false));
          }
          else {
            contextPair = new Pair<>(new Pair<>(true, false), new Pair<>(true, false));
          }
        }

        return contextPair;
      }
    return null;
  }
  
  public static List<Long> getEnterpriseIds(List<Long> orgIds, DvceContext ctx){
    Collection<OrganizationRow> orgRows = OrganizationCacheManager.getInstance().getRowsByIds(orgIds,ctx);
    if(!orgRows.isEmpty()) {
    return  orgRows.stream().map(org->org.getSysEntId()).distinct().collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  /**
   * 
   * To get DS list from RS.
   *
   * @param reqSchedule
   * @return delScheduleList
   */
  public static List<DeliverySchedule> getAllDeliverySchedules(RequestSchedule reqSchedule) {
    List<DeliverySchedule> delScheduleList = new ArrayList<DeliverySchedule>();
    for (final DeliverySchedule delSchedule : reqSchedule.getDeliverySchedules()) {
      delScheduleList.add(delSchedule);
    }
    return delScheduleList;
  }
  
  /**
   * 
   * To get DS list from RS.
   *
   * @param reqSchedule
   * @return delScheduleList
   */
  public static List<Long> getAllItemIds(EnhancedOrder order) {
    List<Long> itemIds = new ArrayList<Long>();
    for (final OrderLine ordLine : order.getOrderLines()) {
    	if(!FieldUtil.isNull(ordLine.getSysItemId()))
    	itemIds.add(ordLine.getSysItemId());
    }
    return itemIds;
  }

  public static List<DeliverySchedule> getAllDeliverySchedules(EnhancedOrder order) {
    Boolean isComplexItemOnReceipt = (Boolean) order.getTransientField(OmsConstants.COMPLEX_ITEM_TRANSIENT_FIELD);
    if (isComplexItemOnReceipt == null)
      isComplexItemOnReceipt = false;
    List<DeliverySchedule> delScheduleList = new ArrayList<DeliverySchedule>();
    for (OrderLine line : order.getOrderLines()) {
      if (OrderLineTypeEnum.COMPLEX.stringValue().equals(line.getLineType()) && !isComplexItemOnReceipt)
        continue;
      for (RequestSchedule reqSchedule : line.getRequestSchedules()) {
        for (DeliverySchedule delSchedule : reqSchedule.getDeliverySchedules()) {
          delScheduleList.add(delSchedule);
        }
      }
    }
    return delScheduleList;
  }

  public static List<DeliverySchedule> getAllDeliverySchedules(OrderLine line) {
    List<DeliverySchedule> delScheduleList = new ArrayList<>();
    for (RequestSchedule reqSchedule : line.getRequestSchedules()) {
      for (DeliverySchedule delSchedule : reqSchedule.getDeliverySchedules()) {
        delScheduleList.add(delSchedule);
      }
    }
    return delScheduleList;
  }

  public static List<DeliverySchedule> getAllDeliverySchedules(OrderLine line,List<String> statesToIgnore) {
    List<DeliverySchedule> delScheduleList = OrderUtil.getAllDeliverySchedules(line);
    delScheduleList = delScheduleList.stream().filter(ds -> !statesToIgnore.contains(ds.getState())).collect(Collectors.toList());
    return delScheduleList;
  }
  
  public static void forEachDeliverySchedule(EnhancedOrder order, Consumer<DeliverySchedule> action) {
    for (OrderLine line : order.getOrderLines()) {
      for (RequestSchedule rs : line.getRequestSchedules()) {
        for (DeliverySchedule ds : rs.getDeliverySchedules()) {
          action.accept(ds);
        }
      }
    }
  }

  public static void forEachDeliverySchedule(List<EnhancedOrder> orders, Consumer<DeliverySchedule> action) {
    for (EnhancedOrder order : orders) {
      forEachDeliverySchedule(order, action);
    }
  }
  
  public static Stream<DeliverySchedule> streamAllDeliverySchedules(EnhancedOrder order) {
    return order.getOrderLines().stream()
      .flatMap(line -> line.getRequestSchedules().stream())
      .flatMap(rs -> rs.getDeliverySchedules().stream());
  }

  /**
   * 
   * To get RS list from Order Line.
   *
   * @param line
   * @return reqScheduleList
   */
  public static List<RequestSchedule> getAllRequestSchedules(final OrderLine line) {
    final List<RequestSchedule> reqScheduleList = new ArrayList<RequestSchedule>();
    for (final RequestSchedule reqSchedule : line.getRequestSchedules()) {
      reqScheduleList.add(reqSchedule);
    }
    return reqScheduleList;
  }
  
  /**
   * 
   * Find Matching deliveryDate.
   *
   * @param line
   * @return reqScheduleList
   */
  public static boolean validateDateFields(DeliverySchedule ds, List<DeliverySchedule> dsList , Boolean includeTime) {
    boolean matched = false;
    Calendar cal = Calendar.getInstance();
    Calendar cal2 = Calendar.getInstance();
    for (DeliverySchedule deliverySchedule : dsList) {
      if(includeTime && deliverySchedule.getRequestDeliveryDate().equals(ds.getRequestDeliveryDate())) {
        matched=true;
        break;
      }
      if(!includeTime) {
        cal.setTime(deliverySchedule.getRequestDeliveryDate().getTime());
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal2.setTime(ds.getRequestDeliveryDate().getTime());
        cal2.set(Calendar.HOUR_OF_DAY, 0);
        cal2.set(Calendar.MINUTE, 0);
        cal2.set(Calendar.SECOND, 0);
        cal2.set(Calendar.MILLISECOND, 0);
        if(cal.equals(cal2)) {
          matched=true;
          break;
        }
      }
    }
    return matched;
  }
  
  /**
   * 
   * Find Matching deliveryDate.
   *
   * @param line
   * @return reqScheduleList
   */
  public static boolean validateDateFields(Calendar date, List<DeliverySchedule> dsList , Boolean includeTime) {
    boolean matched = false;
    Calendar cal = Calendar.getInstance();
    Calendar cal2 = Calendar.getInstance();
    for (DeliverySchedule deliverySchedule : dsList) {
      if(includeTime && deliverySchedule.getRequestDeliveryDate().equals(date)) {
        matched=true;
        break;
      }
      if(!includeTime) {
        cal.setTime(deliverySchedule.getRequestDeliveryDate().getTime());
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        cal2.setTime(date.getTime());
        cal2.set(Calendar.HOUR_OF_DAY, 0);
        cal2.set(Calendar.MINUTE, 0);
        cal2.set(Calendar.SECOND, 0);
        cal2.set(Calendar.MILLISECOND, 0);
        if(cal.equals(cal2)) {
          matched=true;
          break;
        }
      }
    }
    return matched;
  }

  /**
   * 
   * To get RS list from Order.
   *
   * @param order
   * @return reqScheduleList
   */
  public static List<RequestSchedule> getAllRequestSchedules(EnhancedOrder order) {
    List<RequestSchedule> reqScheduleList = new ArrayList<RequestSchedule>();
    for (OrderLine line : order.getOrderLines()) {
      for (RequestSchedule reqSchedule : line.getRequestSchedules()) {
        reqScheduleList.add(reqSchedule);
      }
    }
    return reqScheduleList;

  }
  
  /**
   * 
   * To get order line list from Order.
   *
   * @param order
   * @return orderLineList
   */
  public static List<OrderLine> getAllOrderLinesFromDB(EnhancedOrder order, PlatformUserContext ctx) {
    List<OrderLine> orderLineList = new ArrayList<OrderLine>();
    ModelDataService modelDataService = Services.get(ModelDataService.class); 
    EnhancedOrder orderFromDB = modelDataService.readById(EnhancedOrder.class, order.getSysId(), ctx);
    for (OrderLine line : orderFromDB.getOrderLines()) {
      orderLineList.add(line);
    }
    return orderLineList;
  }
  
  public static void moveStatesToOpen(EnhancedOrder inputOrder, PlatformUserContext ctx) {
    for (OrderLine line : inputOrder.getOrderLines()) {
      if (!OrderUtil.nonCollaborationStates.contains(line.getState())) {
        for (RequestSchedule reqSchedule : line.getRequestSchedules()) {
          if (!OrderUtil.nonCollaborationStates.contains(reqSchedule.getState())) {
            for (DeliverySchedule delSchedule : OrderUtil.getAllDeliverySchedules(reqSchedule)) {//getAllDeliverySchedule(inputOrder)
              String deliveryState = States.OPEN;
              if (!OrderUtil.nonCollaborationStates.contains(delSchedule.getState())
                && !OrderUtil.inFullFillmentStates.contains(delSchedule.getState())) {
                try {
                  if (FieldUtil.isNull(delSchedule.getPromiseDeliveryDate())
                    && !FieldUtil.isNull(delSchedule.getRequestDeliveryDate()))
                    delSchedule.setPromiseDeliveryDate(delSchedule.getRequestDeliveryDate());
                  if (FieldUtil.isNull(delSchedule.getPromiseQuantity()))
                    delSchedule.setPromiseQuantity(delSchedule.getRequestQuantity());
                  if (FieldUtil.isNull(delSchedule.getPromiseShipDate())
                    && !FieldUtil.isNull(delSchedule.getRequestShipDate()))
                    delSchedule.setPromiseShipDate(delSchedule.getRequestShipDate());
                  if ((!delSchedule.isSetPromiseUnitPriceAmount() || FieldUtil.isNull(delSchedule.getPromiseUnitPriceAmount()))
                    && !FieldUtil.isNull(delSchedule.getRequestUnitPriceAmount())) {
                    delSchedule.setPromiseUnitPriceAmount(delSchedule.getRequestUnitPriceAmount());
                    delSchedule.setPromiseUnitPriceUOM(delSchedule.getRequestUnitPriceUOM());
                  }
                  if (FieldUtil.isNull(delSchedule.getPromiseItemName())
                    && FieldUtil.isNull(delSchedule.getSysPromiseItemId())) {
                    delSchedule.setSysPromiseItemId(delSchedule.getParent().getParent().getSysItemId(), true);
                  }
                  delSchedule.setState(States.OPEN);
                  OrderUtil.setDeliveryScheduleStateAndAgreedValues(
                    Collections.singletonList(delSchedule),
                    deliveryState);
                }
                catch (Exception e) {
                  if (!inputOrder.isSetError()) {
                    inputOrder.setError("OMS.enhancedOrder.CollaborationTypeWarn");
                  }
                  LOG.error("Exception :", e);
                }
              }
            }
            reqSchedule.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(reqSchedule)));
          }
        }
        line.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(line)));
      }
    }
    inputOrder.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(inputOrder)));
  }
  

  public static void setupOrderState(List<EnhancedOrder> orders, String state) {
    for (EnhancedOrder order : orders) {
      setupOrderState(order, state);
    }
  }

  public static void setupOnlyOrderState(EnhancedOrder order, String targetState) {
    String originalHeaderState = order.getState();
    if (!OrderUtil.nonTransitionalStates.contains(originalHeaderState) || FieldUtil.isNull(originalHeaderState)) {
      order.setState(targetState);
    }
  }

  /**
   *
   * @param inputOrders
   * @param order
   * @param targetState
   */
  public static void setupOrderState(EnhancedOrder order, String targetState,boolean allowNonTransitionalState) {
    if(allowNonTransitionalState) {
      String originalHeaderState = order.getState();
      if (SCCEnhancedOrderConstants.States.CLOSED.equals(originalHeaderState)) {
        order.setState(targetState);
        for (OrderLine orderLine : order.getOrderLines()) {
          if (SCCEnhancedOrderConstants.States.CLOSED.equals(orderLine.getState())) {
            orderLine.setState(targetState);
            for(RequestSchedule rs:orderLine.getRequestSchedules()) {
              if (SCCEnhancedOrderConstants.States.CLOSED.equals(rs.getState())) {
                rs.setState(targetState);
                for(DeliverySchedule ds:rs.getDeliverySchedules()) {
                  if (SCCEnhancedOrderConstants.States.CLOSED.equals(ds.getState())) {
                    ds.setState(targetState);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  public static void setupOrderState(EnhancedOrder order, String targetState) {
    String originalHeaderState = order.getState();
    if (!OrderUtil.nonTransitionalStates.contains(originalHeaderState) 
      || FieldUtil.isNull(originalHeaderState)
      || (targetState.equalsIgnoreCase(States.CANCELLED) && !invalidStatesForCancel.contains(originalHeaderState))
      || (targetState.equalsIgnoreCase(States.CLOSED)
        && OrderUtil.nonTransitionalStates.contains(originalHeaderState))) {
      order.setState(targetState);
      setupLinesState(order.getOrderLines(), targetState);
    }
  }

  public static void setupLinesState(List<OrderLine> lines, String targetState) {
    for (OrderLine orderLine : lines) {
      String originalLineState = orderLine.getState();
      if (!OrderUtil.nonTransitionalStates.contains(originalLineState) || FieldUtil.isNull(originalLineState)
        || (targetState.equalsIgnoreCase(States.CANCELLED) && !invalidStatesForCancel.contains(originalLineState))
        || (targetState.equalsIgnoreCase(States.CLOSED)
          && !OrderUtil.nonCollaborationStates.contains(originalLineState))) {
        orderLine.setState(targetState);
        if (targetState.equalsIgnoreCase(States.CANCELLED) || targetState.equalsIgnoreCase(States.DELETED))
          orderLine.setTransientField("isStateChanged", true);
        setupRequestScheduleState(orderLine.getRequestSchedules(), targetState);
      }
    }
  }

  public static void setupOrderPromiseStatus(EnhancedOrder order, String targetStatus) {
    String originalPromiseStatus = order.getPromiseStatus();
    if (Objects.isNull(originalPromiseStatus)
      || (Objects.nonNull(targetStatus) && !originalPromiseStatus.equals(targetStatus))) {
      order.setPromiseStatus(targetStatus);
      for (OrderLine ol : order.getOrderLines()) {
        for (RequestSchedule rs : ol.getRequestSchedules()) {
          setupOrderDSPromiseStatus(rs.getDeliverySchedules(), targetStatus);
        }
      }
    }
  }

  public static void setupOrderDSPromiseStatus(List<DeliverySchedule> delSchedules, String targetStatus) {
    for (DeliverySchedule ds : delSchedules) {
      ds.setDsPromiseStatus(targetStatus);
    }
  }

  public static void setupRequestScheduleState(List<RequestSchedule> reqSchedules, String targetState) {
    for (RequestSchedule request : reqSchedules) {
      String originalRequestState = request.getState();
      if (!OrderUtil.nonTransitionalStates.contains(originalRequestState) || FieldUtil.isNull(originalRequestState)
        || (targetState.equalsIgnoreCase(States.CANCELLED) && !invalidStatesForCancel.contains(originalRequestState))
        || (targetState.equalsIgnoreCase(States.CLOSED)
          && OrderUtil.nonTransitionalStates.contains(originalRequestState))) {
        request.setState(targetState);
        setupDeliveryScheduleState(request.getDeliverySchedules(), targetState);
      }
    }
  }

  public static void setupDeliveryScheduleState(List<DeliverySchedule> delSchedules, String targetState) {
    for (DeliverySchedule delSchedule : delSchedules) {
      String originalRequestState = delSchedule.getState();
      if (!OrderUtil.nonTransitionalStates.contains(originalRequestState) || FieldUtil.isNull(originalRequestState)
        || (targetState.equalsIgnoreCase(States.CANCELLED) && !invalidStatesForCancel.contains(originalRequestState))
        || (targetState.equalsIgnoreCase(States.CLOSED)
          && OrderUtil.nonTransitionalStates.contains(originalRequestState))) {
        delSchedule.setState(targetState);  
      }
      if ((targetState.equalsIgnoreCase(States.CANCELLED) && !invalidStatesForCancel.contains(originalRequestState))
    	        || (targetState.equalsIgnoreCase(States.CLOSED))) {
        if(!FieldUtil.isNull(delSchedule.getEmissionAmount())) {
    	  delSchedule.setEmissionAmount(0);
          delSchedule.setEmissionUOM(null);
      }
    }
  }
  }

  public static int getMaxPoLineNumber(EnhancedOrder po) {
    int curNumber = po.getOrderLines().size();
    for (OrderLine line : po.getOrderLines()) {
      int lineNo = 0;
      try {
        lineNo = Integer.parseInt(line.getLineNumber());
      }
      catch (Exception e) {
        //ignore
      }
      if (lineNo > curNumber)
        curNumber = lineNo;
    }
    return curNumber + 1;
  }

  public static void assignLineNumber(
    List<EnhancedOrder> inputOrders,
    List<EnhancedOrder> currentOrders,
    boolean generateLineNumberOnlyForNewLines,
    Map<String, List<OmsOrgProcurmntPolicyRow>> orgPolicyMap,
    PlatformUserContext ctx) {

    if (Objects.isNull(currentOrders))
      return;

    for (EnhancedOrder inputEO : inputOrders) {
      Boolean acceptUploadedOrderNo = OrgProcPolicyUtil.getBooleanPropertyValue(
        orgPolicyMap,
        OrgProcPolicyConstants.ACCEPT_UPLOADED_ORDER_NUMBER,
        inputEO,
        null,
        OrgProcPolicyConstants.YES);
      String poLineNoAssignmentPolicy = OrgProcPolicyUtil.getStringPropertyValue(
        orgPolicyMap,
        OrgProcPolicyConstants.PO_LINE_NO_ASSIGNMENT,
        inputEO,
        null,
        OrgProcPolicyConstants.ONE_NET_VALUE);
      String poNoAssignmentPolicy = OrgProcPolicyUtil.getStringPropertyValue(
        orgPolicyMap,
        OrgProcPolicyConstants.PO_NO_ASSIGNMENT,
        inputEO,
        null,
        OrgProcPolicyConstants.ONE_NET_VALUE);

      int index = currentOrders.indexOf(inputEO);
      if (index < 0)
        continue;
      EnhancedOrder currentEO = currentOrders.get(index);
      List<OrderLine> newLines = getNewRecords(inputEO.getOrderLines(), currentEO.getOrderLines(), "OrderLine");

      if (isContract(inputEO)) {
        List<OrderLine> lines = generateLineNumberOnlyForNewLines ? newLines : inputEO.getOrderLines();
        if (poLineNoAssignmentPolicy.equals(OrgProcPolicyConstants.CONTRACT_TERMS_COUNTER)) {
          if (poNoAssignmentPolicy.equals(OrgProcPolicyConstants.BLOCKS_FROM_EXTERNAL_SYSTEM))
            LOG.warn(
              "PO number is generated using blocks from external system and po line number is generated using contract terms counter");
          OmsContractTermsKey key = getContractTermsKey(inputEO, currentEO);
          if (key != null) {
            final ContractTermsDao termsDao = new ContractTermsDao();
            int existingPOLineNumbers = OrderUtil.getMaxPoLineNumber(inputEO);
            //Get & populate the Numbers
            lines = lines.stream().filter(line -> OrderUtil.shouldGenerateLineNumber(line,acceptUploadedOrderNo)).collect(toList());
            int lineCnt = lines.size();
            OrderNumberResult numberRes = termsDao.getNextPOAndLineNumbers(key, 0, lineCnt);
            int curPoLineNumber = numberRes.getPoLineStartNumber();
            curPoLineNumber=existingPOLineNumbers>curPoLineNumber?existingPOLineNumbers:curPoLineNumber;
            for (OrderLine line : lines) {
              if (curPoLineNumber > 999) {
                line.setError("EO Line number exceeded 999", new Object[] {});
              }
              if (OrderUtil.shouldGenerateLineNumber(line,acceptUploadedOrderNo)) {
                setLineNumber(line, curPoLineNumber++, ctx);
              }
            }
          }
        }
      }

      if (newLines.size() > 0) {
        //if the policy is one net value
        if (!isContract(inputEO) || poLineNoAssignmentPolicy.equals(OrgProcPolicyConstants.ONE_NET_VALUE)) {
          int nextLineNumber = getMaxPoLineNumber(currentEO);
          for (OrderLine line : newLines) {
            if (!acceptUploadedOrderNo || shouldGenerateLineNumber(line,acceptUploadedOrderNo)) {
              setLineNumber(line, nextLineNumber++, ctx);
            }
          }
        }
        populateOriginalValues(newLines, ctx);
      }
    }
  }

  /**
   * populate original value fields
   *
   * @param newLines
   */
  public static void populateOriginalValues(List<OrderLine> newLines, PlatformUserContext ctx) {
    for (OrderLine line : newLines) {
      for (RequestSchedule rs : line.getRequestSchedules()) {
        if (rs.getDeliverySchedules() != null && !rs.getDeliverySchedules().isEmpty()) {
          for (DeliverySchedule ds : rs.getDeliverySchedules()) {
            if (FieldUtil.isNull(ds.getQuantityUOM())) {
              if (!FieldUtil.isNull(line.getQuantityUOM()))
                ds.setQuantityUOM(line.getQuantityUOM());
              else {
                ds.setQuantityUOM(
                  getQuantityUOM(line.getSysItemId(), rs.getSysShipToSiteId(), rs.getSysShipToLocationId(), ctx));
              }
            }
          }

          DeliverySchedule ds = rs.getDeliverySchedules().get(0);
          if (FieldUtil.isNull(rs.getOrigRequestDeliveryDate())) {
            rs.setOrigRequestDeliveryDate(ds.getRequestDeliveryDate());
          }
          if (FieldUtil.isNull(rs.getOrigRequestShipDate()) && FieldUtil.isNull(rs.getOrigRequestShipDate())) {
            rs.setOrigRequestShipDate(ds.getRequestShipDate());
          }
          rs.setOrigRequestQuantity(ds.getRequestQuantity());
          if (FieldUtil.isNull(line.getQuantityUOM()))
            line.setQuantityUOM(ds.getQuantityUOM());

          if (FieldUtil.isNull(rs.getOriginalRequestQuantityUOM())) {
            rs.setOriginalRequestQuantityUOM(ds.getQuantityUOM());
          }
        }
      }
    }
  }

  private static OmsContractTermsKey getContractTermsKey(EnhancedOrder inputEO, EnhancedOrder currentEO) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "OmsContractTermsKey")) {
      EnhancedOrderMDFs poCurMdfs = currentEO.getMDFs(EnhancedOrderMDFs.class);

      String contTermsNo = poCurMdfs.getContractTermsNumber();
      String contNo = poCurMdfs.getContractNumber();
      String contCrtEnt = poCurMdfs.getContractCreationOrganizationEnterpriseName();
      String contCrtOrg = poCurMdfs.getContractCreationOrganizationName();

      if (contTermsNo == null) {
        EnhancedOrderMDFs eoInputMdfs = inputEO.getMDFs(EnhancedOrderMDFs.class);
        contTermsNo = eoInputMdfs.getContractTermsNumber();
        contNo = eoInputMdfs.getContractNumber();
        contCrtEnt = eoInputMdfs.getContractCreationOrganizationEnterpriseName();
        contCrtOrg = eoInputMdfs.getContractCreationOrganizationName();
      }

      if (contTermsNo == null) {
        return null;
      }
      long contractOrgId = OMSUtil.getOrganization(
        new OrganizationKey(inputEO.getValueChainId(), contCrtEnt, contCrtOrg)).getSysOrgId();
      return new OmsContractTermsKey(inputEO.getValueChainId(), contNo, contTermsNo, contractOrgId);
    }
  }

  public static <T extends Model> List<T> getNewRecords(List<T> inputList, List<T> currentList, String modelLevel) {
    final List<T> newList = new ArrayList<T>();
    final List<T> clearCurrent = new ArrayList<T>();
    for (T t : currentList) {
      String state = NullConstants.NULL_STRING_VALUE;
      if (modelLevel.equals("OrderLine")) {
        state = ((OrderLine) t).getState();
      }
      else if (modelLevel.equals("RequestSchedule")) {
        state = ((RequestSchedule) t).getState();
      }
      else if (modelLevel.equals("DeliverySchedule")) {
        state = ((DeliverySchedule) t).getState();
      }
      if (!SCCEnhancedOrderConstants.States.DELETED.equals(state)) {
        clearCurrent.add(t);
      }
    }
    for (T t : inputList) {
      String state = NullConstants.NULL_STRING_VALUE;
      if (modelLevel.equals("OrderLine")) {
        state = ((OrderLine) t).getState();
      }
      else if (modelLevel.equals("RequestSchedule")) {
        state = ((RequestSchedule) t).getState();
      }
      else if (modelLevel.equals("DeliverySchedule")) {
        state = ((DeliverySchedule) t).getState();
      }
      if (!SCCEnhancedOrderConstants.States.DELETED.equals(state)
        && (clearCurrent.isEmpty() || !clearCurrent.contains(t))) {
        newList.add(t);
      }
    }
    return newList;
  }

  public static void assignLineStateForNewLines(
    List<EnhancedOrder> inputOrders,
    List<EnhancedOrder> currentOrders,
    String targetState) {
    for (EnhancedOrder inputPO : inputOrders) {
      int index = currentOrders.indexOf(inputPO);
      if (index < 0)
        continue;
      EnhancedOrder currentPO = currentOrders.get(index);
      List<OrderLine> newLines = getNewRecords(inputPO.getOrderLines(), currentPO.getOrderLines(), "OrderLine");
      if (newLines.size() > 0) {
        for (OrderLine line : newLines) {
          if (!SCCEnhancedOrderConstants.States.DELETED.equals(line.getState())
            && !SCCEnhancedOrderConstants.States.CANCELLED.equals(line.getState())
            && !SCCEnhancedOrderConstants.States.CLOSED.equals(line.getState())) {
            line.setState(targetState);
          }
        }
      }
    }
  }

  public static boolean isContract(EnhancedOrder order) {
    EnhancedOrderMDFs orderMDFs = order.getMDFs(EnhancedOrderMDFs.class);
    if (!order.isIsSpot()
      && (!FieldUtil.isNull(orderMDFs.getSysContractId()) || (!FieldUtil.isNull(orderMDFs.getContractNumber()))))
      return true;
    return false;
  }

  /**
   * Get Planner code From Buffer and if its not set then Item.
   *
   * @param order
   * @param orderLine
   * @param itemRow
   * @return
   */
  public static String getPlannerCodeFromMasterData(OrderLine orderLine, ItemRow itemRow, PlatformUserContext context) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getPlannerCodeFromMasterData")) {
      String plannerCode = null;
      if (null == orderLine) {
        return null;
      }

      try {

        //Retrieving Planner code from Buffer
        Buffer buffer = null;
        for (RequestSchedule rs : orderLine.getRequestSchedules()) {
          if (rs.getSysShipToSiteId() != null) {
            buffer = TransactionCache.getBuffer(itemRow.getSysItemId(), rs.getSysShipToSiteId(), context);
            if (buffer != null) {
              plannerCode = buffer.getMDFs(
                com.ordermgmtsystem.supplychaincore.model.BufferDetailMDFs.class).getPlannerCode();
              break;
            }
          }
        }
        if (buffer == null) {
          return null;
        }

        // If Planner code is not set on buffer Check Planner code from item
        if (FieldUtil.isNull(plannerCode)) {
          if (itemRow != null) {
            plannerCode = itemRow.getPlannerCode();
          }
        }
      }
      catch (Exception e) {
        LOG.error("Failed to get planner code for Order :" + orderLine.getParent().getOrderNumber());
        if (LOG.isDebugEnabled()) {
          e.printStackTrace();
        }
      }
      return plannerCode;
    }
  }

  public static void setDateByCollaborationType(
    DeliverySchedule input,
    DateTypeEnum type,
    EnhancedOrder order,
    Calendar value,
    DvceContext dvceCtx)
    throws Exception {

    PartnerRow partner = null;

    if (!OrderUtil.isDeploymentOrder(order)) {
      partner = PartnerUtil.getVendorPartnerRow(order, dvceCtx);
    }

    Integer collabType = partner == null ? null : partner.getOmsCollaborationType();

    if (collabType == null || collabType == CollaborationByEnum.DELIVERYDATE.intValue()) {

      if (type.equals(DateTypeEnum.REQUEST))
        input.setRequestDeliveryDate(value);
      else if (type.equals(DateTypeEnum.AGREED))
        input.setAgreedDeliveryDate(value);
      else if (type.equals(DateTypeEnum.PROMISED))
        input.setPromiseDeliveryDate(value);
      else
        throw new Exception("Unknown date type");
    }
    else if (collabType.equals(CollaborationByEnum.SHIPDATE.intValue())) {
      if (type.equals(DateTypeEnum.REQUEST))
        input.setRequestShipDate(value);
      else if (type.equals(DateTypeEnum.AGREED))
        input.setAgreedShipDate(value);
      else if (type.equals(DateTypeEnum.PROMISED))
        input.setPromiseShipDate(value);
      else
        throw new Exception("Unknown date type");
    }
    else
      throw new Exception("Unknown collaboration type value");
  }

  public static String getDateNameByCollaborationType(EnhancedOrder order, PlatformUserContext ctx) throws Exception {
    Integer collabType = 0;
    boolean isNMD = false;
    OrderLine orderLine = order.getOrderLines().get(0);
    if (!FieldUtil.isNull(orderLine.getExtItemName())) {
      isNMD = true;
    }

    if (!isNMD && !OrderUtil.isDeploymentOrder(order)) {
      collabType = PartnerUtil.getPartnerCollabTypeAsInt(order, (DvceContext) ctx);
    }

    //null means default is delivery date
    if (collabType == null || collabType == CollaborationByEnum.DELIVERYDATE.intValue())
      return "promised delivery date";
    else if (collabType.equals(CollaborationByEnum.SHIPDATE.intValue()))
      return "promised ship date";
    else
      throw new Exception("Unknown collaboration type value");
  }

  public static Calendar getDateByCollaboration(
    DeliverySchedule input,
    DateTypeEnum type,
    EnhancedOrder order,
    DvceContext dvceCtx)
    throws Exception {
    PartnerRow partner = null;

    if (!OrderUtil.isDeploymentOrder(order)) {
      partner = PartnerUtil.getVendorPartnerRow(order, dvceCtx);
    }

    Integer collabType = partner == null ? null : partner.getOmsCollaborationType();
    //null means default is delivery date

    if (collabType == null || collabType == CollaborationByEnum.DELIVERYDATE.intValue()) {
      if (type.equals(DateTypeEnum.REQUEST))
        return input.getRequestDeliveryDate();
      if (type.equals(DateTypeEnum.AGREED))
        return input.getAgreedDeliveryDate();
      if (type.equals(DateTypeEnum.PROMISED))
        return input.getPromiseDeliveryDate();
      throw new Exception("Unknown date type");
    }
    else if (collabType.equals(CollaborationByEnum.SHIPDATE.intValue())) {
      if (type.equals(DateTypeEnum.REQUEST))
        return OrderDateUtil.getShipOrRequestDate(input.getRequestShipDate(), input.getRequestDeliveryDate());
      if (type.equals(DateTypeEnum.AGREED))
        return OrderDateUtil.getShipOrRequestDate(input.getAgreedShipDate(), input.getAgreedDeliveryDate());
      if (type.equals(DateTypeEnum.PROMISED))
        return OrderDateUtil.getShipOrRequestDate(input.getPromiseShipDate(), input.getPromiseDeliveryDate());
      throw new Exception("Unknown date type");
    }
    else
      throw new Exception("Unknown collaboration type value");
  }

  public static boolean isEmailVendorForOrder(EnhancedOrder order, PlatformUserContext ctx) {
    PartnerKey key = new PartnerKey(
      order.getVendorName(),
      order.getValueChainId(),
      order.getBuyingOrgEnterpriseName(),
      order.getBuyingOrgName());
    PartnerRow row = PartnerUtil.getPartner(key, ctx);
    if (row == null || FieldUtil.isNull(row.getOmsCommunicationMode())) {
      key = new PartnerKey(
        order.getSellingOrgName(),
        order.getValueChainId(),
        order.getBuyingOrgEnterpriseName(),
        order.getBuyingOrgName());
      row = PartnerUtil.getPartner(key, ctx);
    }
    if (row == null || FieldUtil.isNull(row.getOmsCommunicationMode()))
      return false;
    else if (row.getOmsCommunicationMode().equals(CommunicationTypeEnum.EMAIL.intValue())
      || row.getOmsCommunicationMode().equals(CommunicationTypeEnum.PORTAL.intValue()))
      return true;
    else
      return false;
  }

  /**
   * Returns a comma separated string of order numbers
   * in the list
   *
   * @param orders list of {EnhancedOrder}
   * @return comma separated string of order numbers
   */
  public static String getOrderNumbers(List<EnhancedOrder> orders) {
    if (orders == null || orders.isEmpty()) {
      return "No orders";
    }
    StringBuilder sb = new StringBuilder();
    for (EnhancedOrder order : orders) {
      sb.append(order.getOrderNumber()).append(",");
    }
    sb.setLength(sb.length() - 1);
    return sb.toString();
  }

  /**
   * Gets order sysIds from <code>orders</code>
   * doesn't read data from database, just gets them from
   * jaxb objects if they're present
   *
   * @param orders list of {@link EnhancedOrder}
   * @return
   */
  public static List<Long> getOrderIds(List<EnhancedOrder> orders) {
    if (orders == null || orders.isEmpty()) {
      return Collections.emptyList();
    }
    List<Long> ids = new ArrayList<Long>();
    for (EnhancedOrder order : orders) {
      if (!FieldUtil.isNull(order.getSysId())) {
        ids.add(order.getSysId());
      }
    }
    return ids;
  }

  /**
   * Gets order by sysId from the <code>orders</code>
   *
   * @param id sysId
   * @param orders list of orders
   * @return {@link EnhancedOrder}
   */
  public static EnhancedOrder getOrderByIdFromList(Long id, List<EnhancedOrder> orders) {
    if (orders == null || orders.isEmpty()) {
      return null;
    }
    for (EnhancedOrder o : orders) {
      if (!FieldUtil.isNull(o.getSysId()) && !FieldUtil.isNull(id) && id.equals(o.getSysId())) {
        return o;
      }
    }
    return null;
  }

  public static double getValidQty(DeliverySchedule delSchedule) {
    if (delSchedule.getAgreedQuantity() > 0)
      return delSchedule.getAgreedQuantity();
    else if (delSchedule.getPromiseQuantity() > 0)
      return delSchedule.getPromiseQuantity();
    else
      return delSchedule.getRequestQuantity();
  }

  public static double getValidQtyForContractNetting(DeliverySchedule delSchedule, boolean isEnableCollabPerDs) {
    if (delSchedule.getTransientField("isRequestQtyChanged") != null)
      return delSchedule.getRequestQuantity();
    else if (delSchedule.isSetAgreedQuantity() && !FieldUtil.isNull(delSchedule.getAgreedQuantity())) {
      if (!isEnableCollabPerDs) {
        double agreedQty = 0.0;
        RequestSchedule rs = delSchedule.getParent();
        for (DeliverySchedule ds : rs.getDeliverySchedules()) {
          if (!States.VENDOR_REJECTED.equalsIgnoreCase(ds.getState())) {
            agreedQty = agreedQty + ds.getAgreedQuantity();
          }
        }
        return agreedQty;
      }
      else {
        if (!States.VENDOR_REJECTED.equalsIgnoreCase(delSchedule.getState()))
          return delSchedule.getAgreedQuantity();
        else
          return 0.0;
      }
    }
    else if (!States.VENDOR_REJECTED.equalsIgnoreCase(delSchedule.getState()))
      return delSchedule.getRequestQuantity();
    else
      return 0.0;
  }

  public static double getTotalValidQty(RequestSchedule requestSchedule) {
    double totalValidQty = 0.0;
    for (DeliverySchedule ds : requestSchedule.getDeliverySchedules()) {
      if (!OrderUtil.nonTransitionalStates.contains(ds.getState()))
        totalValidQty += getValidQty(ds);
    }
    return totalValidQty;
  }

  public static double getInFullFillmentPromiseQty(RequestSchedule requestSchedule) {
    double inFullFillmentPromiseQty = 0.0;
    for (DeliverySchedule ds : requestSchedule.getDeliverySchedules()) {
      if (inFullFillmentStates.contains(ds.getState()))
        inFullFillmentPromiseQty += getValidQty(ds);
    }
    return inFullFillmentPromiseQty;
  }

  public static double getTotalNettedReceivedQuantityForRS(DeliverySchedule deliverySchedule) {
    double nettedReceivedQty = 0.0;
    RequestSchedule rs = deliverySchedule.getParent();
    for (DeliverySchedule ds : rs.getDeliverySchedules()) {
      DeliveryScheduleMDFs dsMDFs = ds.getMDFs(DeliveryScheduleMDFs.class);
      nettedReceivedQty = nettedReceivedQty + dsMDFs.getNettedReceivedQty();
    }
    return nettedReceivedQty;
  }

  public static double getTotalPromiseQty(RequestSchedule requestSchedule) {
    double totalPromiseQty = 0.0;
    for (DeliverySchedule ds : requestSchedule.getDeliverySchedules()) {
      if (!OrderUtil.nonCollaborationStates.contains(ds.getState()))
        totalPromiseQty += ds.getPromiseQuantity();
    }
    return totalPromiseQty;
  }

  public static Model convertAvlLineRowToJaxb(IDaoRow row, DvceContext dvceContext) throws VCBaseException {
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled()) {
        psrKey = PSRLogger.enter("OrderUtil.convertAvlLineRowToJaxb");
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Converting row to JAXB: " + row);
      }

      AvlLineRow avlLineRow = (AvlLineRow) row;
      AvlLine avlLine = new AvlLine();

      try {
        JaxbSpyUtil.populateJaxbFromDaoRow(avlLine, avlLineRow, AvlLine.STANDARD_MODEL_NAME, dvceContext);
      }
      catch (EnumerationException e) {
        throw new VCBaseException(e);
      }

      return avlLine;
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
  }

  public static List<AclLine> getAclLines(EnhancedOrder order, PlatformUserContext context) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getAclLines")) {
      List<AclLine> aclList = new ArrayList<AclLine>();
      try {
        ModelDataService mService = Services.get(ModelDataService.class);
        List<Long> itemIds = new ArrayList<Long>();
        for (OrderLine line : order.getOrderLines()) {
          if (!FieldUtil.isNull(line.getSysItemId())) {
            itemIds.add(line.getSysItemId());
          }
        }
        if (!itemIds.isEmpty()) {
          SqlParams params = new SqlParams();
          params.setLongValue("SELLER_ORG_ID", order.getSysSellingOrgId());
          params.setLongValue("BUYER_ORG_ID", order.getSysBuyingOrgId());
          params.setCollectionValue("ITEM_IDS", itemIds);

          String filterSql = null;
          if (order.getOrderType().equals(OrderTypeEnum.SALES_ORDER.stringValue())) {
            filterSql = "SYS_ORGANIZATION_ID = $SELLER_ORG_ID$ AND SYS_CUSTOMER_ORGANIZATION_ID = $BUYER_ORG_ID$"
              + "AND ACTIVE = 1 AND SYS_ITEM_ID in $ITEM_IDS$";
          }
          else {
            filterSql = "SYS_ORGANIZATION_ID = $SELLER_ORG_ID$ AND SYS_CUSTOMER_ORGANIZATION_ID = $BUYER_ORG_ID$"
              + "AND ACTIVE = 1 AND SYS_CUSTOMER_ITEM_ID in $ITEM_IDS$";
          }

          aclList = mService.read(AclLine.class, context, params, ModelQuery.sqlFilter(filterSql));
        }
      }
      catch (Exception e) {
        order.setError("OMS.enhancedOrder.validation.cannotReadACL");
      }
      if (aclList != null && !aclList.isEmpty())
        return aclList;
      else {
        return new ArrayList<AclLine>();
      }
    }
  }

  public static boolean isAutoFlagEnabledForLine(OrderLine line, String autoFlag, PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "isAutoFlagEnabledForLine")) {
      EnhancedOrder order = line.getParent();

      for (RequestSchedule rs : line.getRequestSchedules()) {
        AvlLine avlLine = OrderUtil.getAVLFromRS(rs, ctx);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Has Auto flag " + autoFlag + "enabled on Partner ? " + false);
          LOG.debug(
            "AVL Line: [ Partner =" + avlLine.getPartnerName() + " Item Name =" + avlLine.getItemName() + " Site Name ="
              + avlLine.getSiteName() + "]");
        }
        if (null != avlLine) {
          AvlLineMDFs avlLineMDFs = avlLine.getMDFs(AvlLineMDFs.class);
          if (OMS_AUTO_PO_ACK_MODE.equals(autoFlag)) {
            if (!FieldUtil.isNull(avlLineMDFs.getAutoPoAckMode()))
              return true;
          }
          else if (OMS_AUTO_CLOSE_ON_RECEIPT.equals(autoFlag)) {
            if (FieldUtil.isDifferent(avlLineMDFs.isAutoCloseOnReceipt(), false)) {
              return true;
            }
          }
        }
      }

      PartnerRow partnerRow = PartnerUtil.getPartner(order.getSysVendorId(), ctx);
      if (OMS_AUTO_PO_ACK_MODE.equals(autoFlag)) {
        if (partnerRow != null && partnerRow.getIsActive() == 1
          && !FieldUtil.isNull(partnerRow.getOmsAutoPoAckMode())) {
          return true;
        }
      }
      else if (partnerRow != null && partnerRow.getIsActive() == 1 && OMS_AUTO_CLOSE_ON_RECEIPT.equals(autoFlag)) {
        if (FieldUtil.isDifferent(partnerRow.isOmsAutoCloseOnReceipt(), false)) {
          return true;
        }
      }

      LOG.debug("Auto flag enabled ? " + false);
      return false;
    }
  }

  public static void setState(Model model, String state, String errorMessage) {
    if (model instanceof DeliverySchedule) {
      ((DeliverySchedule) model).setState(state);
      if (state == null)
        ((DeliverySchedule) model).setError(errorMessage);
    }
    else if (model instanceof RequestSchedule) {
      ((RequestSchedule) model).setState(state);
      if (state == null)
        ((RequestSchedule) model).setError(errorMessage);
    }
    else if (model instanceof OrderLine) {
      ((OrderLine) model).setState(state);
      if (state == null)
        ((OrderLine) model).setError(errorMessage);
    }
    else if (model instanceof EnhancedOrder) {
      ((EnhancedOrder) model).setState(state);
      if (state == null)
        ((EnhancedOrder) model).setError(errorMessage);
    }
  }

  /**
   * This method copies details from current DS to input DS so that those details not get ovveridden during the cancel operation.
   *
   * @param deliverySchedule
   * @param currentDeliverySchedule
   */
  public static void copyOverDetailsFromCurrent(
    DeliverySchedule deliverySchedule,
    DeliverySchedule currentDeliverySchedule) {
    if (currentDeliverySchedule != null) {
      DeliveryScheduleMDFs inputOmsDsMdfs = deliverySchedule.getMDFs(DeliveryScheduleMDFs.class);
      DeliveryScheduleMDFs currentOmsDsMdfs = currentDeliverySchedule.getMDFs(DeliveryScheduleMDFs.class);
      deliverySchedule.setRequestQuantity(currentDeliverySchedule.getRequestQuantity());
      deliverySchedule.setPromiseQuantity(currentDeliverySchedule.getPromiseQuantity());
      deliverySchedule.setRequestDeliveryDate(currentDeliverySchedule.getRequestDeliveryDate());
      deliverySchedule.setPromiseDeliveryDate(currentDeliverySchedule.getPromiseDeliveryDate());
      deliverySchedule.setRequestShipDate(currentDeliverySchedule.getRequestShipDate());
      deliverySchedule.setPromiseShipDate(currentDeliverySchedule.getPromiseShipDate());

      deliverySchedule.setRequestUnitPriceAmount(currentDeliverySchedule.getRequestUnitPriceAmount());
      if (currentDeliverySchedule.isSetPromiseUnitPriceAmount()) {
        deliverySchedule.setPromiseUnitPriceAmount(currentDeliverySchedule.getPromiseUnitPriceAmount());
      }
      if (!FieldUtil.isNull(currentOmsDsMdfs.getRequestPricePer()) && currentOmsDsMdfs.getRequestPricePer() > 0) {
        inputOmsDsMdfs.setPromisePricePer(currentOmsDsMdfs.getRequestPricePer());
      }
      if (!FieldUtil.isNull(currentOmsDsMdfs.getPromisePricePer()) && currentOmsDsMdfs.getPromisePricePer() > 0) {
        inputOmsDsMdfs.setPromisePricePer(currentOmsDsMdfs.getPromisePricePer());
      }
      deliverySchedule.setRequestUnitPriceUOM(currentDeliverySchedule.getRequestUnitPriceUOM());
      deliverySchedule.setPromiseUnitPriceUOM(currentDeliverySchedule.getPromiseUnitPriceUOM());

      deliverySchedule.setRequestIncoDateStartDate(currentDeliverySchedule.getRequestIncoDateStartDate());
      deliverySchedule.setPromiseIncoDateStartDate(currentDeliverySchedule.getPromiseIncoDateStartDate());

      deliverySchedule.setRequestMinItemExpiryDate(currentDeliverySchedule.getRequestMinItemExpiryDate());
      deliverySchedule.setPromiseMinItemExpiryDate(currentDeliverySchedule.getPromiseMinItemExpiryDate());

    }
  }

  public static void copyShipFromSiteFromSibling(
    EnhancedOrder inputOrder,
    EnhancedOrder currentOrder,
    PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "copyShipFromSiteFromSibling")) {
    long lineSysShipFromSiteId = 0l;
    long rsSysShipFromSiteId = 0l;
    long dsSysShipFromSiteId = 0l;
    long shipFromSiteId = 0l;
    for (OrderLine line : inputOrder.getOrderLines()) {
      rsSysShipFromSiteId = 0l;
      for (RequestSchedule rs : line.getRequestSchedules()) {
        if (!OrderUtil.nonCollaborationStates.contains(rs.getState())) {
          dsSysShipFromSiteId = 0l;
          for (DeliverySchedule ds : rs.getDeliverySchedules()) {
            if (!OrderUtil.nonCollaborationStates.contains(ds.getState())) {
              if (FieldUtil.isNull(ds.getSysShipFromSiteId()) && FieldUtil.isNull(ds.getShipFromAddress())) {
                DeliverySchedule currentDS = OrderUtil.findMatchingDeliverySchedule(currentOrder, ds);
                if (dsSysShipFromSiteId == 0 && currentDS != null
                  && !FieldUtil.isNull(currentDS.getSysShipFromSiteId())) {
                  dsSysShipFromSiteId = currentDS.getSysShipFromSiteId();
                }
                //Copy only for new DS
                if (currentDS == null) {
                  shipFromSiteId = dsSysShipFromSiteId != 0
                    ? dsSysShipFromSiteId
                    : (rsSysShipFromSiteId != 0 ? rsSysShipFromSiteId : lineSysShipFromSiteId);
                  if (shipFromSiteId != 0) {
                    SiteRow site = SiteCacheManager.getInstance().getRow(shipFromSiteId, (DvceContext) ctx);
                    if (site != null) {
                      ds.setShipFromSiteEnterpriseName(site.getEntName());
                      ds.setShipFromSiteOrganizationName(site.getOrgName());
                      ds.setShipFromSiteName(site.getSiteName());
                      ds.setSysShipFromSiteId(site.getSysSiteId(), false);
                    }
                  }
                }
              }
            }
          }
        }
        rsSysShipFromSiteId = dsSysShipFromSiteId != 0 ? dsSysShipFromSiteId : rsSysShipFromSiteId;
      }
      lineSysShipFromSiteId = rsSysShipFromSiteId != 0 ? rsSysShipFromSiteId : lineSysShipFromSiteId;
    }
    }
  }

  public static boolean isActionAllowedByAcceptanceMode(
    Pair<BuyerAcceptanceEnum, AutoPoAckModeEnum> acceptanceMode,
    String actionName) {
    boolean isActionAllowed = true;
    if (!Objects.isNull(acceptanceMode)) {
      final BuyerAcceptanceEnum buyerAcceptance = acceptanceMode.first;
      final AutoPoAckModeEnum vendorAcceptance = acceptanceMode.second;
      if (!Objects.isNull(buyerAcceptance)) {
        switch (buyerAcceptance) {
          case ACCEPT_TO_NEW_OPEN_DISALLOW_CHANGE:
          case ACCEPT_TO_OPEN_DISALLOW_CHANGE:
            if (actionName.equalsIgnoreCase(SCCEnhancedOrderConstants.Actions.BUYER_CHANGE_REQUEST)
              || actionName.equalsIgnoreCase(SCCEnhancedOrderConstants.Actions.CONSIGNEE_CHANGE_REQUEST)
              || actionName.equalsIgnoreCase(REJECT_VENDOR_CHANGES)
              || actionName.equalsIgnoreCase(REJECT_SHIPPER_CHANGES)) {
              isActionAllowed = false;
            }
            break;
        }
      }
      if (isActionAllowed && !Objects.isNull(vendorAcceptance)) {
        switch (vendorAcceptance) {
          case ACCEPT_TO_OPEN_DISALLOW_CHANGE:
            if (actionName.equalsIgnoreCase(SCCEnhancedOrderConstants.Actions.VENDOR_CHANGE_REQUEST)
              || actionName.equalsIgnoreCase(SCCEnhancedOrderConstants.Actions.SHIPPER_CHANGE_REQUEST)
              || actionName.equalsIgnoreCase(SCCEnhancedOrderConstants.Actions.VENDOR_CONFIRM)
              || actionName.equalsIgnoreCase(SCCEnhancedOrderConstants.Actions.SHIPPER_CONFIRM)
              || actionName.equalsIgnoreCase(SCCEnhancedOrderConstants.Actions.VENDOR_REJECT)) {
              isActionAllowed = false;
            }
            break;
        }
      }
    }
    return isActionAllowed;
  }

  public static boolean isAutoAccepted(Pair<BuyerAcceptanceEnum, AutoPoAckModeEnum> acceptanceMode, String actionName) {
    boolean isAutoAccepted = false;
    if (!Objects.isNull(acceptanceMode)) {
      switch (actionName) {
        case SCCEnhancedOrderConstants.Actions.VENDOR_PROMISE:
        case SCCEnhancedOrderConstants.Actions.VENDOR_CHANGE_REQUEST:
        case SCCEnhancedOrderConstants.Actions.SHIPPER_CHANGE_REQUEST:
        case SCCEnhancedOrderConstants.Actions.VENDOR_CONFIRM:
        case SCCEnhancedOrderConstants.Actions.SHIPPER_CONFIRM:
          if (!Objects.isNull(acceptanceMode.first)
            && (acceptanceMode.first.equals(BuyerAcceptanceEnum.ACCEPT_TO_NEW_OPEN_ALLOW_CHANGE)
              || acceptanceMode.first.equals(BuyerAcceptanceEnum.ACCEPT_TO_NEW_OPEN_DISALLOW_CHANGE)
              || acceptanceMode.first.equals(BuyerAcceptanceEnum.ACCEPT_TO_OPEN_ALLOW_CHANGE)
              || acceptanceMode.first.equals(BuyerAcceptanceEnum.ACCEPT_TO_OPEN_DISALLOW_CHANGE))) {
            isAutoAccepted = true;
          }
          if (SCCEnhancedOrderConstants.Actions.VENDOR_PROMISE.equals(actionName)
             && !Objects.isNull(acceptanceMode.second) && (
               acceptanceMode.second.equals(AutoPoAckModeEnum.ACCEPT_TILL_OPEN)
            || acceptanceMode.second.equals(AutoPoAckModeEnum.ACCEPT_TO_OPEN_ALLOW_CHANGE)
            || acceptanceMode.second.equals(AutoPoAckModeEnum.ACCEPT_TO_OPEN_DISALLOW_CHANGE))) {
            isAutoAccepted = true;
          }
          break;
        case SCCEnhancedOrderConstants.Actions.BUYER_CHANGE_REQUEST:
        case SCCEnhancedOrderConstants.Actions.CONSIGNEE_CHANGE_REQUEST:
        case SCCEnhancedOrderConstants.Actions.BUYER_CONFIRM:
        case SCCEnhancedOrderConstants.Actions.CONSIGNEE_CONFIRM:
        case OrderUtil.REJECT_VENDOR_CHANGES:
        case OrderUtil.REJECT_SHIPPER_CHANGES:
          if (!Objects.isNull(acceptanceMode.second) && (
            acceptanceMode.second.equals(AutoPoAckModeEnum.ACCEPT_FROM_OPEN)
            || acceptanceMode.second.equals(AutoPoAckModeEnum.ACCEPT_TO_OPEN_ALLOW_CHANGE)
            || acceptanceMode.second.equals(AutoPoAckModeEnum.ACCEPT_TO_OPEN_DISALLOW_CHANGE))) {
            isAutoAccepted = true;
          }
          break;
      }
    }
    return isAutoAccepted;
  }

  public static boolean isErrorSetOnChildren(RequestSchedule requestSchedule) {
    boolean isErrorSet = false;
    for (DeliverySchedule deliverySchedule : requestSchedule.getDeliverySchedules()) {
      if (deliverySchedule.getError() != null)
        isErrorSet = true;
    }
    return isErrorSet;
  }

  public static boolean isErrorSetOnChildren(OrderLine orderLine) {
    boolean isErrorSet = false;
    for (RequestSchedule requestSchedule : orderLine.getRequestSchedules()) {
      isErrorSet = isErrorSetOnChildren(requestSchedule);
    }
    return isErrorSet;
  }

  public static boolean isErrorSetOnChildren(EnhancedOrder order) {
    boolean isErrorSet = false;
    for (OrderLine orderLine : order.getOrderLines()) {
      isErrorSet = isErrorSetOnChildren(orderLine);
    }
    return isErrorSet;
  }

  public static String getEffectiveOrderHeaderState(List<String> states, String calculatedState) {
    if (SCCEnhancedOrderConstants.States.BACKORDERED.equals(calculatedState)) {
      if (states.contains(SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES))
        calculatedState = SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES;
      else if (states.contains(SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED))
        calculatedState = SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED;
      else if (states.contains(SCCEnhancedOrderConstants.States.OPEN)) {
        calculatedState = SCCEnhancedOrderConstants.States.OPEN;
      }
      else
        calculatedState = SCCEnhancedOrderConstants.States.BACKORDERED;
    }
    return calculatedState;
  }

  public static String getEffectiveState(List<String> states) {

    int isInFulfillment = 0;
    int isInTransit = 0;
    int isInPromising = 0;
    int isInNew = 0;
    int isPartiallyReceived = 0;
    int isReceived = 0;
    int isCancelled = 0;
    int isConverted = 0;
    int isClosed = 0;
    int isVendorRejected = 0;
    int isOpen = 0;
    int isDeleted = 0;
    int isBackOrdered = 0;
    int vendorCR = 0;
    int vendorCWC = 0;
    int buyerCR = 0;
    int buyerCWC = 0;
    int isPartiallyShipped = 0;
    int awaitingApproval = 0;
    int countableSchedules = 0;
    int nonTransCount = 0;
    String targetState = null;

    for (String state : states) {
      if(state == null) continue;
      
      if (com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.IN_PROMISING.equals(state))
        isInPromising++;
      if (SCCEnhancedOrderConstants.States.NEW.equals(state))
        isInNew++;
      else if (SCCEnhancedOrderConstants.States.AWAITING_APPROVAL.equals(state))
        awaitingApproval++;
      else if (SCCEnhancedOrderConstants.States.CANCELLED.equals(state))
        isCancelled++;
      else if (SCCEnhancedOrderConstants.States.CONVERTED.equals(state))
        isConverted++;
      else if (SCCEnhancedOrderConstants.States.VENDOR_REJECTED.equals(state))
        isVendorRejected++;
      else if (SCCEnhancedOrderConstants.States.BACKORDERED.equals(state))
        isBackOrdered++;
      else if (SCCEnhancedOrderConstants.States.DELETED.equals(state))
        isDeleted++;
      else if (SCCEnhancedOrderConstants.States.CLOSED.equals(state))
        isClosed++;
      else if (SCCEnhancedOrderConstants.States.IN_FULFILLMENT.equals(state))
        isInFulfillment++;
      else if (SCCEnhancedOrderConstants.States.PARTIALLY_SHIPPED.equals(state))
        isPartiallyShipped++;
      else if (SCCEnhancedOrderConstants.States.IN_TRANSIT.equals(state))
        isInTransit++;
      else if (SCCEnhancedOrderConstants.States.PARTIALLY_RECEIVED.equals(state))
        isPartiallyReceived++;
      else if (SCCEnhancedOrderConstants.States.RECEIVED.equals(state))
        isReceived++;
      else if (SCCEnhancedOrderConstants.States.OPEN.equals(state))
        isOpen++;
      else if (SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED.equals(state))
        buyerCR++;
      else if (SCCEnhancedOrderConstants.States.VENDOR_CHANGE_REQUESTED.equals(state))
        vendorCR++;
      else if (SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES.equals(state))
        buyerCWC++;
      else if (SCCEnhancedOrderConstants.States.VENDOR_CONFIRMED_WITH_CHANGES.equals(state))
        vendorCWC++;
      countableSchedules++;
    }

    nonTransCount = isCancelled + isVendorRejected + isDeleted + isConverted;

    if (countableSchedules > 0) {

      if (isCancelled == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.CANCELLED;
      if (isDeleted == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.DELETED;
      else if (isVendorRejected == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.VENDOR_REJECTED;
      else if (isVendorRejected > 0 && nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.VENDOR_REJECTED;
      else if (nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.CANCELLED;
      else if (isClosed + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.CLOSED;
      else if (isInNew + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.NEW;
      else if (isInPromising + nonTransCount == countableSchedules)
        targetState = com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.IN_PROMISING;
      else if (buyerCR + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED;
      else if (isBackOrdered + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.BACKORDERED;
      else if (vendorCR + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.VENDOR_CHANGE_REQUESTED;
      else if (buyerCWC + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES;
      else if (vendorCWC + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.VENDOR_CONFIRMED_WITH_CHANGES;
      else if (isInFulfillment + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.IN_FULFILLMENT;
      else if (isInTransit + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.IN_TRANSIT;
      else if (isOpen + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.OPEN;
      else if (isPartiallyShipped + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.PARTIALLY_SHIPPED;
      else if (isPartiallyReceived + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.PARTIALLY_RECEIVED;
      else if (isReceived + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.RECEIVED;
      else if (isReceived + isClosed + nonTransCount == countableSchedules)
        targetState = SCCEnhancedOrderConstants.States.RECEIVED;
      else if (isPartiallyReceived + isReceived + isClosed > 0)
        targetState = SCCEnhancedOrderConstants.States.PARTIALLY_RECEIVED;
      else if (isInTransit + isPartiallyShipped > 0)
        targetState = SCCEnhancedOrderConstants.States.PARTIALLY_SHIPPED;
      else if (isInFulfillment > 0)
        targetState = SCCEnhancedOrderConstants.States.IN_FULFILLMENT;
      else if (vendorCR > 0)
        targetState = SCCEnhancedOrderConstants.States.VENDOR_CHANGE_REQUESTED;
      else if (buyerCR > 0)
        targetState = SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED;
      else if (vendorCWC > 0)
        targetState = SCCEnhancedOrderConstants.States.VENDOR_CONFIRMED_WITH_CHANGES;
      else if (buyerCWC > 0)
        targetState = SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES;
      else if (isOpen > 0)
        targetState = SCCEnhancedOrderConstants.States.OPEN;
      else if (isBackOrdered > 0)
        targetState = SCCEnhancedOrderConstants.States.BACKORDERED;
      else if (isInPromising > 0)
        targetState = com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.IN_PROMISING;
      else if (isInNew > 0)
        targetState = SCCEnhancedOrderConstants.States.NEW;
      else if (awaitingApproval >0)
        targetState = SCCEnhancedOrderConstants.States.AWAITING_APPROVAL;
      else
        targetState = null;
    }
    return targetState;
  }

  /**
   * Check whether the RequestSchedule is Partial BackOrdered. 
   * 
   * @param requestSchedule
   * @return isRSHasBackOrderedDS = true, if promise quantity is less than request quantity and backordered quantity is greater than zero.
   */
  public static boolean isRSContainsPartiallyBackOrderedDS(final RequestSchedule rs) {
    boolean isRSHasBackOrderedDS = false;
    for (final DeliverySchedule ds : rs.getDeliverySchedules())
      if (States.BACKORDERED.equalsIgnoreCase(ds.getState()) && ds.getBackOrderQuantity() > DvceConstants.EPSILON
        && ds.getBackOrderQuantity() != ds.getRequestQuantity()
        && !States.BACKORDERED.equalsIgnoreCase(rs.getState())) {
        isRSHasBackOrderedDS = true;
        break;
      }
    return isRSHasBackOrderedDS;
  }
  
  public static void clearErrorForSync(List<EnhancedOrder> orders) {
	  for(EnhancedOrder order : orders) {
		  order.setError(null);
		  for(OrderLine line : order.getOrderLines()) {
			  line.setError(null);
			  for(RequestSchedule rs : line.getRequestSchedules()) {
				  rs.setError(null);
				  for(DeliverySchedule ds : rs.getDeliverySchedules()) {
					  ds.setError(null);
				  }
			  }
		  }
	  }
  }

  public static boolean isRSFullyBackOrdered(final RequestSchedule rs) {
    boolean isRSFullyBackOrdered = false;
    for (final DeliverySchedule ds : rs.getDeliverySchedules())
      if (States.BACKORDERED.equalsIgnoreCase(ds.getState())
        && !FieldUtil.isDifferent(ds.getRequestQuantity(), ds.getBackOrderQuantity())) {
        isRSFullyBackOrdered = true;
        break;
      }
    return isRSFullyBackOrdered;
  }
  
  public static boolean isIntegRSContainsBackOrderedDS(final RequestSchedule rs, OrderLine currentOrderLine) {
    boolean isIntegRSBackordered = false;
    if(!rs.isBuyerNotAcceptable() &&  (rs.getParent().getParent().getOrigin().equals(OriginEnum.UIUPLOAD.stringValue()) 
      || rs.getParent().getParent().getOrigin().equalsIgnoreCase(OriginEnum.INTEG.stringValue()) 
      || rs.getParent().getParent().getOrigin().equalsIgnoreCase(OriginEnum.EDI.stringValue()))) {
      boolean hasCurrentBackorderedDS = currentOrderLine.getRequestSchedules().stream().flatMap(crs -> crs.getDeliverySchedules().stream()).anyMatch(cds -> States.BACKORDERED.equalsIgnoreCase(cds.getState()));
      if(hasCurrentBackorderedDS && rs.getDeliverySchedules().size() == 1 && !FieldUtil.isNull(rs.getDeliverySchedules().get(0).getBackOrderQuantity())) {
        isIntegRSBackordered = true;
      }
    }
    return isIntegRSBackordered;
  }

  public static Long getStorageSiteValue(EnhancedOrder order, Map<RequestSchedule, AvlLine> avlLines, PlatformUserContext ctx) {
    if (FieldUtil.isNull(order.getSysOwningOrgId()) || FieldUtil.isNull(order.getSysSellingOrgId())) {
      LOG.error("Something wrong Owning Org or Selling Org is null");
      return null;
    }
    //PSR fix: Check if storage Site AVL's are available before fetching storage site per order line
    Long storageSite = (Long) order.getTransientField("getStorageSiteValue");
    if (Objects.nonNull(storageSite)) {
      return FieldUtil.isNull(storageSite) ? null : storageSite;
    }
    if (!isStorageSiteCheckNeeded(order)) {
      order.setTransientField("getStorageSiteValue", DvceConstants.NULL_LONG_VALUE);
      return null;
    }
    for (OrderLine line : order.getOrderLines()) {
      if (line.getRequestSchedules().isEmpty() && line.getRequestSchedules().size() <= 0) {
        return null;
      }

      AvlLine avlLine = null;
      if(avlLines == null) {
    	  avlLine= OrderUtil.getAVLFromRS(line.getRequestSchedules().get(0), ctx);
      } else {
    	  avlLine = avlLines.get(line.getRequestSchedules().get(0));
      }
      if (avlLine != null && !FieldUtil.isNull(avlLine.getSysStorageSiteId())) {
        return avlLine.getSysStorageSiteId();
      }
    }
    order.setTransientField("getStorageSiteValue", DvceConstants.NULL_LONG_VALUE);
    return null;
  }

  /**
   * Check if there are any AVL's available in the system with Storage Site populated
   * 
   *
   * @param order
   * @return true if storage site AVL's are available in the system
   */
  private static boolean isStorageSiteCheckNeeded(EnhancedOrder order) {
    SqlParams params = new SqlParams();
    params.setLongValue("OWNING_ORG_ID", order.getSysOwningOrgId());
    params.setLongValue("VENDOR_ORG_ID", order.getSysSellingOrgId());
    SqlResult result = Services.get(SqlService.class).executeQuery(
      "select 1 from avl_line where org_id =$OWNING_ORG_ID$ and VENDOR_ORG_ID=$VENDOR_ORG_ID$ and SYS_STORAGE_SITE_ID is not null",
      params);
    return !result.getRows().isEmpty();
  }

  public static String getQuantityUOM(
    Long itemId,
    Long sysShipToSiteId,
    Long sysShipToLocationId,
    PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getQuantityUOM")) {
      Item item = null;
      Buffer bufferRow = null;
      if (itemId != null)
        item = TransactionCache.getItem(itemId, ctx);

      if (item != null) {
        bufferRow = TransactionCache.getBuffer(item.getSysId(), sysShipToSiteId, sysShipToLocationId, ctx);
      }
      if(bufferRow != null && OrderUtil.getSCCBufferMdfs(bufferRow) != null
    	        && !FieldUtil.isNull(OrderUtil.getSCCBufferMdfs(bufferRow).getOrderUOM())) {
    	  return OrderUtil.getSCCBufferMdfs(bufferRow).getOrderUOM();
      }
      
      if (item != null && !FieldUtil.isNull(com.ordermgmtsystem.supplychaincore.model.ItemMDFs.from(item).getOrderUOM())) {
          return com.ordermgmtsystem.supplychaincore.model.ItemMDFs.from(item).getOrderUOM();
      }

      if (bufferRow != null && OrderUtil.getSCCBufferMdfs(bufferRow) != null
        && !FieldUtil.isNull(OrderUtil.getSCCBufferMdfs(bufferRow).getOrderingUOM())) {
        return OrderUtil.getSCCBufferMdfs(bufferRow).getOrderingUOM();
      }
      if (item != null && !FieldUtil.isNull(item.getOrderingUOM())) {
        return item.getOrderingUOM();
      }
      LOG.warn("No default order uom available - defalting to EACH");
      return OMSUtil.getEnumStr(EnumerationType.QUANTITY_UOM, QuantityUOM.EACH.toInteger());
      }
    }

  public static boolean isDeploymentOrder(EnhancedOrder order) {
    return OrderTypeEnum.DEPLOYMENT_ORDER.stringValue().equals(order.getOrderType());
  }

  public static boolean isPurchaseOrder(EnhancedOrder order) {
    return OrderTypeEnum.PURCHASE_ORDER.stringValue().equals(order.getOrderType());
  }

  public static boolean isDOCollaborationEnabled(EnhancedOrder order, PlatformUserContext ctx) {
    Policy<Boolean> policy = Policy.ENABLE_COLLABORATION_FOR_DO;
    Boolean isCollabEnabled = (Boolean) order.getTransientField(policy.getName());
    if (isCollabEnabled != null) {
      return isCollabEnabled;
    }
    isCollabEnabled = getOrderSitePolicy(order, policy, ctx);
    order.setTransientField(policy.getName(), isCollabEnabled);
    return isCollabEnabled;
  }
  

  public static List<String> getExistingChildrenStatesModel(Model currentModel, EnhancedOrder inputOrder) {
    final List<String> states = new ArrayList<String>();
    if (currentModel instanceof RequestSchedule)
      for (DeliverySchedule ds : ((RequestSchedule) currentModel).getDeliverySchedules()) {
        DeliverySchedule inputDeliverySchedule = findMatchingDeliverySchedule(inputOrder, ds);
        if (inputDeliverySchedule == null)
          states.add(((DeliverySchedule) ds).getState());
      }
    if (currentModel instanceof OrderLine)
      for (RequestSchedule rs : ((OrderLine) currentModel).getRequestSchedules()) {
        RequestSchedule inputReqestSchedule = findMatchingRequestSchedule(inputOrder, rs);
        if (inputReqestSchedule == null)
          states.add(((RequestSchedule) rs).getState());
      }

    if (currentModel instanceof EnhancedOrder)
      for (OrderLine ol : ((EnhancedOrder) currentModel).getOrderLines()) {
        OrderLine inputOrderLine = findMatchingOrderLine(inputOrder, ol);
        if (inputOrderLine == null)
          states.add(((OrderLine) ol).getState());
      }
    return states;
  }

  public static List<String> getChildrenStateList(Model model) {
    final List<String> states = new ArrayList<String>();
    if (model instanceof RequestSchedule)
      for (DeliverySchedule ds : ((RequestSchedule) model).getDeliverySchedules())
        states.add(((DeliverySchedule) ds).getState());

    if (model instanceof OrderLine)
      for (RequestSchedule rs : ((OrderLine) model).getRequestSchedules())
        states.add(((RequestSchedule) rs).getState());

    if (model instanceof EnhancedOrder)
      for (OrderLine ol : ((EnhancedOrder) model).getOrderLines())
        states.add(((OrderLine) ol).getState());

    return states;
  }
  
  public static boolean checkBuyerChangeReasonPolicy(EnhancedOrder inputOrder,PlatformUserContext ctx) {
    boolean enforceBuyerDeviationCode = TransactionCache.getOrgPolicy(
      OMSConstants.Policies.ENFORCE_BUYER_COLLABORATION_REASON_CODE,
      inputOrder.getSysOwningOrgId(),
      false,
      ctx);
    return enforceBuyerDeviationCode;
  }
  
  
  public static List<String> getAllChildrenStateList(EnhancedOrder order) {
	final List<String> states = new ArrayList<String>();
	states.add(order.getState());
	for(OrderLine line: order.getOrderLines()) {
	    states.add(line.getState());
	    for(RequestSchedule requestSchedule: line.getRequestSchedules()) {
	    	states.add(requestSchedule.getState());
	    	for(DeliverySchedule schedule: requestSchedule.getDeliverySchedules()) {
	    		states.add(schedule.getState());
			}	
		}	
	}
	return states;
  }

  public static List<String> getChildrenStateList(Model model, EnhancedOrder order) {
    final List<String> states = new ArrayList<String>();

    if (model instanceof RequestSchedule) {
      RequestSchedule rs = (RequestSchedule) model;
      for (DeliverySchedule ds : rs.getDeliverySchedules())
        states.add(
          rs.getState() == null
            ? getMatchingCurrentDeliverySchedule(rs.getParent().getParent(), ds) != null
              ? getMatchingCurrentDeliverySchedule(rs.getParent().getParent(), ds).getState()
              : rs.getState()
            : rs.getState());
      RequestSchedule currentRS = findMatchingRequestSchedule(order, rs);
      if (currentRS != null)
        for (DeliverySchedule currentDS : currentRS.getDeliverySchedules())
          if (findMatchingDeliverySchedule(rs.getParent().getParent(), currentDS) == null)
            states.add(currentDS.getState());
    }
    if (model instanceof OrderLine) {
      OrderLine ol = (OrderLine) model;
      for (RequestSchedule rs : ol.getRequestSchedules()) {
        states.add(
          rs.getState() == null
            ? getMatchingCurrentRequestSchedule(order, rs) != null
              ? getMatchingCurrentRequestSchedule(order, rs).getState()
              : rs.getState()
            : rs.getState());
      }
      OrderLine currentOL = findMatchingOrderLine(order, ol);
      if (currentOL != null)
        for (RequestSchedule currentRS : currentOL.getRequestSchedules())
          if (findMatchingRequestSchedule(ol.getParent(), currentRS) == null)
            states.add(currentRS.getState());
    }

    if (model instanceof EnhancedOrder) {
      EnhancedOrder o = (EnhancedOrder) model;
      for (OrderLine ol : o.getOrderLines())
        states.add(ol.getState());

      if (order != null)
        for (OrderLine currentOL : order.getOrderLines())
          if (findMatchingOrderLine(o, currentOL) == null)
            states.add(currentOL.getState());
    }
    return states;
  }

  private static DeliverySchedule getMatchingCurrentDeliverySchedule(
    EnhancedOrder currentOrder,
    DeliverySchedule inputSchedule) {
    RequestSchedule inputRS = inputSchedule.getParent();
    OrderLine inputLine = inputRS.getParent();
    for (OrderLine currentLine : currentOrder.getOrderLines()) {
      for (RequestSchedule reqSchedule : currentLine.getRequestSchedules()) {
        for (DeliverySchedule delSchedule : reqSchedule.getDeliverySchedules()) {
          if (inputLine.getLineNumber().equals(currentLine.getLineNumber())
            && inputRS.getRequestScheduleNumber().equals(reqSchedule.getRequestScheduleNumber())
            && delSchedule.getDeliveryScheduleNumber().equals(inputSchedule.getDeliveryScheduleNumber())) {
            return delSchedule;
          }
        }
      }
    }
    return null;
  }

  private static RequestSchedule getMatchingCurrentRequestSchedule(
    EnhancedOrder currentOrder,
    RequestSchedule inputSchedule) {
    OrderLine inputLine = inputSchedule.getParent();
    for (OrderLine currentLine : currentOrder.getOrderLines()) {
      for (RequestSchedule reqSchedule : currentLine.getRequestSchedules()) {
        if (inputLine.getLineNumber().equals(currentLine.getLineNumber())
          && inputSchedule.getRequestScheduleNumber().equals(reqSchedule.getRequestScheduleNumber())) {
          return reqSchedule;
        }
      }
    }
    return null;
  }

  public static boolean isAVL(EnhancedOrder order) {
    if (!order.isIsSpot() && !isContract(order) && OrderTypeEnum.PURCHASE_ORDER.toString().equals(order.getOrderType()))
      return true;
    return false;
  }

  /** Nullifies Promise values for Engine created orders
   * and calculates the Requested Ship date based on the lead time
   *    * @param orders
   * @param ctx
   */
  public static void nullifyPromiseQty(List<EnhancedOrder> orders, PlatformUserContext ctx) {
    for (EnhancedOrder order : orders) {
      if (isAVL(order)
        && (OriginEnum.IXM.stringValue().equals(order.getOrigin()) || IXM_LOAD_BUILDER.equals(order.getOrigin()))) {
        Map<DeliverySchedule, Long> leadTimes = OrderDateUtil.getLeadTimes(getAllDeliverySchedules(order), ctx);
        for (OrderLine orderLine : order.getOrderLines()) {
          for (RequestSchedule requestSchedule : orderLine.getRequestSchedules()) {
            for (DeliverySchedule deliverySchedule : requestSchedule.getDeliverySchedules()) {
              if (!FieldUtil.isNull(deliverySchedule.getRequestDeliveryDate())
                && !FieldUtil.isNull(deliverySchedule.getRequestShipDate())
                && deliverySchedule.getRequestDeliveryDate().before(deliverySchedule.getRequestShipDate())) {
                try {
                  Long leadTime = leadTimes.get(deliverySchedule);
                  Long reqShipDate = deliverySchedule.getRequestShipDate().getTimeInMillis();
                  if (leadTime >= 0) {

                    Long delDate = reqShipDate + leadTime;
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(new Date(delDate));
                    TimeZone tz = OrderDateUtil.fetchTimeZone(requestSchedule.getSysShipToSiteId(), ctx);
                    if (null != tz) {
                      cal = TimeZoneUtil.convertTimeToTimeZone(cal, tz);
                    }
                    if (deliverySchedule.getRequestShipDate().before(cal))
                      deliverySchedule.setRequestDeliveryDate(cal);
                  }
                }
                catch (Exception e) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Error in calculating request delivery date" + e.getMessage());
                  }
                }
              }
              deliverySchedule.setOrigPromisedQuantity(Constants.NULL_DOUBLE_VALUE);
              deliverySchedule.setPromiseQuantity(Constants.NULL_DOUBLE_VALUE);
              deliverySchedule.setPromiseDeliveryDate(null);
              deliverySchedule.setPromiseShipDate(null);
            }
          }
        }
      }
    }
  }

  /** 
   * 
   * Populates the FOB code and the payment terms from vendor.
   *
   * @param order
   * @param ctx
   */
  public static void populateFOBCodeAndPaymentTermsFromVendor(EnhancedOrder order, PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(
      PSR_ID,
      OrderUtil.class,
      "populateFOBCodeAndPaymentTermsFromVendor")) {
      if (!(OrderTypeEnum.PURCHASE_ORDER.stringValue().equals(order.getOrderType()) || OrderTypeEnum.RETURN_ORDER.stringValue().equals(order.getOrderType())))
        return;
      PartnerRow row = PartnerUtil.getPartner(order.getSysVendorId(), null);
      if (null == row) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unable to retrieve the vendor");
        }
        return;
      }

      EnhancedOrderMDFs orderMDFs = order.getMDFs(EnhancedOrderMDFs.class);
      // Set FOB Code
      if (!FieldUtil.isNull(row.getOmsFobcode())) {
        if (FieldUtil.isNull(orderMDFs.getFOBCode())) {
          orderMDFs.setFOBCode(row.getOmsFobcode());
          LOG.debug("FOB code from vendor is" + row.getOmsFobcode());
        }
      }

      //Set Payment terms
      Long paymentTermsId = row.getOmsSysPaymentTermsId();
      if (!FieldUtil.isNull(paymentTermsId) && FieldUtil.isNull(orderMDFs.getSysPaymentTermsId())) {
        ModelDataService modelDataService = Services.get(ModelDataService.class); //FIXME : Use SQL Service instead of MDS
        PaymentTerms model = modelDataService.readById(PaymentTerms.class, paymentTermsId, ctx);
        if (model != null && !FieldUtil.isNull(model.getPaymentTermsCode())) {
          orderMDFs.setSysPaymentTermsId(paymentTermsId, false);
          orderMDFs.setPaymentTermsCode(model.getPaymentTermsCode());
          orderMDFs.setPaymentTermsEnterpriseName(model.getEnterpriseName());
          LOG.debug("Payment terms from vendor is" + model.getPaymentTermsCode());
        }
      }
    }
  }

  public static void populateFOBCodeAndPaymentTermsReturnOrder(EnhancedOrder order, PlatformUserContext ctx) {
	  
      if (!FieldUtil.isNull(order.getOrderType())
        && order.getOrderType().equalsIgnoreCase(OrderTypeEnum.RETURN_ORDER.stringValue())
        && !OrderUtil.isContract(order)) {
        Long sysItemId = null;
        sysItemId = order.getOrderLines().get(0).getSysItemId();
        //Contract not present for this order ,so checking in ACL Lines
        List<AclLine> aclLineList = new ArrayList<AclLine>();
        aclLineList = getAclLinesReturn(order, (DvceContext) ctx);
        
        Long tempBuyingOrgId = OrderUtil.getOrgByKeyOptionally(
                order.getSysBuyingOrgId(),
                order.getBuyingOrgEnterpriseName(),
                order.getBuyingOrgName(),
                order.getValueChainId());
              Long tempSellingOrgId = OrderUtil.getOrgByKeyOptionally(
                order.getSysSellingOrgId(),
                order.getSellingOrgEnterpriseName(),
                order.getSellingOrgName(),
                order.getValueChainId());
              
        boolean isVendor = ctx.isDerivedFrom(SCCConstants.RoleTypes.VENDOR_SUPPLY_CHAIN_ADMIN);
        PartnerRow row = (isVendor)?PartnerUtil.getCustomerMasterRow(order.getValueChainId(), tempSellingOrgId, tempBuyingOrgId):PartnerUtil.getPartner(order.getSysVendorId(), null);
        
        
        //Populate payment terms and FOB etc in order from contract
        if (aclLineList != null && !aclLineList.isEmpty() && aclLineList.size() > 0) {
          for (AclLine aclLine : aclLineList) {
            if (!FieldUtil.isNull(sysItemId) && (sysItemId.equals(aclLine.getSysItemId()) || sysItemId.equals(aclLine.getSysCustomerItemId()))) {
              EnhancedOrderMDFs orderMDFs = order.getMDFs(EnhancedOrderMDFs.class);
              if (FieldUtil.isNull(orderMDFs.getSysPaymentTermsId())
                && !FieldUtil.isNull(aclLine.getMDFs(AclLineMDFs.class).getSysPaymentTermsCodeId())) {
                ModelDataService modelDataService = Services.get(ModelDataService.class);
                PaymentTerms model = modelDataService.readById(
                  PaymentTerms.class,
                  aclLine.getMDFs(AclLineMDFs.class).getSysPaymentTermsCodeId(),
                  ctx);
                if (model != null && !FieldUtil.isNull(model.getPaymentTermsCode())) {
                  orderMDFs.setSysPaymentTermsId(aclLine.getMDFs(AclLineMDFs.class).getSysPaymentTermsCodeId(), false);
                  orderMDFs.setPaymentTermsCode(model.getPaymentTermsCode());
                  orderMDFs.setPaymentTermsEnterpriseName(model.getEnterpriseName());
                }
              }
              if (!FieldUtil.isNull(orderMDFs.getFOBCode())
                && !FieldUtil.isNull(aclLine.getMDFs(AclLineMDFs.class).getFOBCode())) {
                orderMDFs.setFOBCode(aclLine.getMDFs(AclLineMDFs.class).getFOBCode());
              }
              if (!FieldUtil.isNull(orderMDFs.getFOBPoint())
                && !FieldUtil.isNull(aclLine.getMDFs(AclLineMDFs.class).getFOBPoint())) {
                orderMDFs.setFOBPoint(aclLine.getMDFs(AclLineMDFs.class).getFOBPoint());
              }
              if (!FieldUtil.isNull(order.getSysFreightFwdOrgId())
                && !FieldUtil.isNull(aclLine.getMDFs(AclLineMDFs.class).getSysFreightForwarderPartnerId())) {
                order.setSysFreightFwdOrgId(aclLine.getMDFs(AclLineMDFs.class).getSysFreightForwarderPartnerId(), false);
                OrganizationRow org = OrganizationCacheManager.getInstance().getOrganization(
                  aclLine.getMDFs(AclLineMDFs.class).getSysFreightForwarderPartnerId());
                if (org != null) {
                  order.setFreightFwdOrgName(org.getOrgName());
                  order.setFreightFwdOrgEnterpriseName(org.getEntName());
                }
              }
              if(FieldUtil.isNull(orderMDFs.getSysPaymentTermsId())
                 && FieldUtil.isNull(aclLine.getMDFs(AclLineMDFs.class).getSysPaymentTermsCodeId())
                 && !FieldUtil.isNull(row.getOmsSysPaymentTermsId())) {
            	  ModelDataService modelDataService = Services.get(ModelDataService.class);
                  PaymentTerms model = modelDataService.readById(
                    PaymentTerms.class,
                    row.getOmsSysPaymentTermsId(),
                    ctx);
                  if (model != null && !FieldUtil.isNull(model.getPaymentTermsCode())) {
                    orderMDFs.setSysPaymentTermsId(row.getOmsSysPaymentTermsId(), false);
                    orderMDFs.setPaymentTermsCode(model.getPaymentTermsCode());
                    orderMDFs.setPaymentTermsEnterpriseName(model.getEnterpriseName());
                  }
              }
              break;
            }
          }
        }
      }
    
}

public static List<AclLine> getAclLinesReturn(EnhancedOrder order, PlatformUserContext context) {
    List<AclLine> aclList = new ArrayList<AclLine>();
    try {
      ModelDataService mService = Services.get(ModelDataService.class);
      List<Long> itemIds = new ArrayList<Long>();
      for (OrderLine line : order.getOrderLines()) {
        if (!FieldUtil.isNull(line.getSysItemId())) {
          itemIds.add(line.getSysItemId());
        }
      }
      if (!itemIds.isEmpty()) {
        SqlParams params = new SqlParams();
        params.setLongValue("SELLER_ORG_ID", order.getSysSellingOrgId());
        params.setLongValue("BUYER_ORG_ID", order.getSysBuyingOrgId());
        params.setCollectionValue("ITEM_IDS", itemIds);

        String filterSql = null;
        if (order.getOrderType().equals(OrderTypeEnum.SALES_ORDER.stringValue())) {
          filterSql = "SYS_ORGANIZATION_ID = $SELLER_ORG_ID$ AND SYS_CUSTOMER_ORGANIZATION_ID = $BUYER_ORG_ID$"
            + "AND ACTIVE = 1 AND SYS_ITEM_ID in $ITEM_IDS$";
        }
        else {
          filterSql = "SYS_ORGANIZATION_ID = $SELLER_ORG_ID$ AND SYS_CUSTOMER_ORGANIZATION_ID = $BUYER_ORG_ID$"
            + "AND ACTIVE = 1 AND SYS_CUSTOMER_ITEM_ID in $ITEM_IDS$";
        }

        aclList = mService.read(AclLine.class, context, params, ModelQuery.sqlFilter(filterSql));
      }
    }
    catch (Exception e) {
      order.setError("OMS.enhancedOrder.validation.cannotReadACL");
    }
    if (aclList != null && !aclList.isEmpty())
      return aclList;
    else {
      return new ArrayList<AclLine>();
    }
  }

  public static boolean isVMIEnabledForOrderVendor(EnhancedOrder order, DvceContext dvceCtx) {
    PartnerRow vendorMasterRow = TransactionCache.getPartnerRow(order.getSysVendorId(), dvceCtx);

    if (vendorMasterRow != null && vendorMasterRow.isVmi()
      && order.getOrderType().equals(OrderTypeEnum.PURCHASE_ORDER.stringValue()) && !order.isIsSpot()) {
      return true;
    }
    else {
      return false;
    }
  }

  public static boolean checkVMIItem(EnhancedOrder order, PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "checkVMIItem")) {
      boolean isValid = false;

      if (isVMIEnabledForOrderVendor(order, (DvceContext) ctx)) {
        return true;
      }
      else if (!order.isIsSpot()) {
        for (OrderLine line : order.getOrderLines()) {
          if (!FieldUtil.isNull(line.getItemName()) && !isContract(order)) {
            for (RequestSchedule rs : line.getRequestSchedules()) {
              if (nonTransitionalStates.contains(rs.getState())) {
                if (order.isIsVMI()) {
                  isValid = true;
                }
                continue;
              }
              AvlLine avlLine = OrderUtil.getAVLFromRS(rs, ctx);
              if (null != avlLine && avlLine.isVMI()) {
                isValid = true;
              }
              else {
                isValid = false;
                break;
              }
            }
          }
        }
      }
      return isValid;
    }
  }

  public static boolean checkInactiveItem(EnhancedOrder order, PlatformUserContext ctx) {
	  try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "checkInactiveItem")) {
		  boolean isInActive= false;
		  for (OrderLine line : order.getOrderLines()) {
			  ItemRow currentItemRow = ItemCacheManager.getInstance().getItem(new ItemKey(order.getValueChainId(), line.getItemEnterpriseName(), line.getItemName()));
			  if( currentItemRow != null && !currentItemRow.isIsActive()) {
				  isInActive=true;
		    	  if (!order.isSetError()) {
		    		  order.setError("OMS.enhancedOrder.InactiveItem", currentItemRow.getItemName());
		    	  }
			  }
		  }
		  return isInActive;
	  }
  }
  
  public static void populateAutoMoveToReceived(List<EnhancedOrder> orders, PlatformUserContext ctx) {
    // Check the policy only once to avoid DB calls. Policy value same for all order as they will have the same owning org
    boolean isMoveToReceipt = OrgProcPolicyUtil.getBooleanValue(
      (DvceContext) ctx,
      ((DvceContext) ctx).getUserContext().getValueChainId(),
      orders.get(0).getSysOwningOrgId(),
      OrgProcPolicyConstants.AUTO_MOVE_TO_RECEIPT_DO,
      false);
    boolean isAutoReceipt = OrgProcPolicyUtil.getBooleanValue(
      (DvceContext) ctx,
      ((DvceContext) ctx).getUserContext().getValueChainId(),
      orders.get(0).getSysOwningOrgId(),
      OrgProcPolicyConstants.AUTO_RECEIPT_DO,
      false);
    for (EnhancedOrder order : orders) {
      EnhancedOrderMDFs orderMDFs = order.getMDFs(EnhancedOrderMDFs.class);
      orderMDFs.setAutoMoveToReceived(isMoveToReceipt);
      order.setIsAutoReceipt(isAutoReceipt);
    }
  }

  public static void populateDefaultLineValues(OrderLine line, Map<RequestSchedule, AvlLine> avlLines,
		  Map<String, List<OmsOrgProcurmntPolicyRow>> orgPolicyMap, PlatformUserContext ctx) {
    String psrKey = null;
    try {
    	EnhancedOrder order = line.getParent();   
      if (PSRLogger.isEnabled()) {
        psrKey = PSRLogger.enter(PSR_ID + "populateDefaultLineValues");
      }
      Long sysShipToLocationId = null;
      boolean isFirstLine = true;

      if (order.getTransientField("isFirstLine") == null) {
        isFirstLine = true;
        order.setTransientField("isFirstLine", true);
      }
      else {
        isFirstLine = false;
      }

      if (nonTransitionalStates.contains(line.getState()))
        return;

      boolean isNMD = false;
      if (!FieldUtil.isNull(line.getExtItemName())) {
        isNMD = true;
      }
      if (!isNMD) {
        Long itemId = null;
        Long sysShipToSiteId = null;
        if (!FieldUtil.isNull(line.getSysItemId())) {
          itemId = line.getSysItemId();
        }
        RequestSchedule rsToBeUsed = null;
        for(RequestSchedule rs: line.getRequestSchedules()) {
          if (nonTransitionalStates.contains(rs.getState()))
            continue;
          if (!FieldUtil.isNull(rs.getSysShipToLocationId())) {
            sysShipToLocationId = rs.getSysShipToLocationId();
          }
          if (!FieldUtil.isNull(rs.getSysShipToSiteId()) && FieldUtil.isNull(sysShipToSiteId)) {
            sysShipToSiteId = rs.getSysShipToSiteId();
            rsToBeUsed = rs;
          }
          if (!FieldUtil.isNull(sysShipToSiteId) && !sysShipToSiteId.equals(rs.getSysShipToSiteId())) {
            LOG.warn("Multiple price were present and we picked the first.");
            break;
          }
        }
        if (isContract(order)) {
          for (RequestSchedule rs : line.getRequestSchedules()) {
            if (!FieldUtil.isNull(sysShipToSiteId) && !sysShipToSiteId.equals(rs.getSysShipToSiteId())) {
              break;
            }
          }

          //if price on order is null populate it from contract line
          ContractResult res = OrderUtil.getContractFromRS(line.getRequestSchedules().get(0), ctx);
          if (res != null && res.getContract() != null) {
            //PDS-49760 : As per comment for this ticket for PO Release Price and Price Per should always populated from Contract.
            if (!FieldUtil.isNull(res.getUnitPrice()) && res.getUnitPrice() > 0.0) {
              line.setUnitPrice(res.getUnitPrice());
              if (OrderTypeEnum.SALES_ORDER.stringValue().equals(order.getOrderType())) {
                line.getRequestSchedules().stream().flatMap(rs -> rs.getDeliverySchedules().stream()).forEach(ds -> {
                  ds.setRequestUnitPriceAmount(res.getUnitPrice());
                });
              }
            }
            
            if(!FieldUtil.isNull(res.getOmsPricePer()) && res.getOmsPricePer() > 0.0) {
              line.getMDFs(OrderLineMDFs.class).setPricePer(res.getOmsPricePer());
              if (OrderTypeEnum.SALES_ORDER.stringValue().equals(order.getOrderType())) {
                line.getRequestSchedules().stream().flatMap(rs -> rs.getDeliverySchedules().stream()).forEach(ds -> {
                  ds.getMDFs(DeliveryScheduleMDFs.class).setRequestPricePer(res.getOmsPricePer());
                });
              }
            }

            if (FieldUtil.isNull(line.getQuantityUOM())) {
              String qtyUOM = "";
              if (FieldUtil.isNull(res.getContractLine().getQtyUom())) {
                qtyUOM = getQuantityUOM(line.getSysItemId(), res.getContractTerm().getSysShipToSiteId(), null, ctx);
              }
              else {
                qtyUOM = res.getContractLine().getQtyUom().toString();
              }
              line.setQuantityUOM(qtyUOM);
            }
          }
          if (res != null && res.getContractTerm() != null) {
            if (!FieldUtil.isNull(res.getContractTerm().getInvoiceActiveCode()))
              order.setIsConsignment(
                res.getContractTerm().getInvoiceActiveCode().equals(InvoiceActiveCodeEnum.CONSIGNMENT.intValue()));
            if (res.getContractTerm().isIsVmi()) {
              order.setIsVMI(true);
            }
            else {
              order.setIsVMI(false);
            }
          }
          order.setIsAutoReceipt(getIsAutoReceipt(order, ctx));
          EnhancedOrderMDFs orderMDFs = order.getMDFs(EnhancedOrderMDFs.class);
          orderMDFs.setAutoMoveToReceived(getIsAutoMoveToReceived(order, ctx));
        }
        else if ((isAVL(order) && !OrderTypeEnum.SALES_ORDER.stringValue().equals(order.getOrderType())
          && !FieldUtil.isNull(line.getSysItemId()) || order.isIsSpot())
          || (!FieldUtil.isNull(order.getOrigin()) && order.getOrigin().contains(OriginEnum.IXM.stringValue()))) {
          setUnitPrice(order, ctx, sysShipToLocationId, line, itemId, sysShipToSiteId);
          setPricePer(order, ctx, sysShipToLocationId, line, itemId, sysShipToSiteId);
        } else if(OrderTypeEnum.DEPLOYMENT_ORDER.stringValue().equals(order.getOrderType())) {
          if (Objects.nonNull(rsToBeUsed) && FieldUtil.isNull(line.getQuantityUOM())) {
              Long rsShipTo = FieldUtil.isNull(rsToBeUsed.getSysShipToSiteId()) ? null : rsToBeUsed.getSysShipToSiteId();
              Long rsShipToLocation = FieldUtil.isNull(rsToBeUsed.getSysShipToLocationId())? null : rsToBeUsed.getSysShipToLocationId();
              line.setQuantityUOM(OrderUtil.getQuantityUOM(line.getSysItemId(), rsShipTo, rsShipToLocation, ctx));
          } else if(FieldUtil.isNull(line.getQuantityUOM())){
            line.setQuantityUOM(OrderUtil.getQuantityUOM(line.getSysItemId(), null, null, ctx));
          }
        }

        AvlLine avlLine = null;
        if (null != rsToBeUsed && isAVL(order)) {
        	if(avlLines != null) {
        		avlLine = avlLines.get(rsToBeUsed);
        	} 
          if(avlLine == null) {
        		avlLine = OrderUtil.getAVLFromRS(rsToBeUsed, ctx);
        	}
          
        }
        if (avlLine != null && FieldUtil.isNull(order.getSysVendorId())
          && OriginEnum.UIUPLOAD.stringValue().equals(order.getOrigin())
          && OrderTypeEnum.PURCHASE_ORDER.stringValue().equals(order.getOrderType()) && order.isIsVMI()) {
          order.setSysVendorId(avlLine.getSysPartnerId(), true);
        }

        if (OrderTypeEnum.DEPLOYMENT_ORDER.stringValue().equals(order.getOrderType())) {
        	Boolean autoReciept = OrgProcPolicyUtil.getBooleanPropertyValue(orgPolicyMap, OrgProcPolicyConstants.AUTO_RECEIPT_DO,
        			order, order.getSysOwningOrgId(), "false");
          if (autoReciept) {
            EnhancedOrderMDFs orderMDFs = order.getMDFs(EnhancedOrderMDFs.class);
            orderMDFs.setAutoMoveToReceived(true);
          }
        }

        boolean isVMIEnabledForVendor = OrderUtil.isVMIEnabledForOrderVendor(order, (DvceContext) ctx);
        if (null != rsToBeUsed && (order.isIsVMI() ||isVMIEnabledForVendor)) {
          AvlLine avl =null;
          if(avlLine == null) {
            if(avlLines != null) {
              avl = avlLines.get(rsToBeUsed);
            } 
            if(avl == null) {
              avl = OrderUtil.getAVLFromRS(rsToBeUsed, ctx);
            }
          } else {
            avl =avlLine;
          }
          
          if(avl != null) {
            populateBPONumber(order, avl, line, isFirstLine);
          }
        }
        if (avlLine != null && !isPlantPullOrder(ctx, avlLines, order)) {
          AvlLineMDFs avlLineMDFs = avlLine.getMDFs(AvlLineMDFs.class);
          if (!isContract(order)) {
            if (!order.isIsSpot()
              && (FieldUtil.isNull(order.getSysRequisitionId()) || order.getSysRequisitionId() <= 0)) {

              if (!order.isIsVMI() && isVMIEnabledForVendor) {
                order.setIsVMI(true);
              }
              else if (!isVMIEnabledForVendor) {
                order.setIsVMI(avlLine.isVMI());
              }
            }
            order.setIsConsignment(avlLine.isConsignment());
          }

          order.setIsAutoReceipt(getIsAutoReceipt(order, ctx));
          EnhancedOrderMDFs orderMDFs = order.getMDFs(EnhancedOrderMDFs.class);
          orderMDFs.setAutoMoveToReceived(getIsAutoMoveToReceived(order, ctx));

          populateBPONumber(order, avlLine, line, isFirstLine);
          if (FieldUtil.isNull(line.getVendorItemName())) {
            boolean isVendorItemSet = false;
            if (!FieldUtil.isNull(avlLineMDFs.getSupplierItemName())) { //Check if vendorItem/supplierItemName is populated in AVL
              OrganizationRow orgRow = OrganizationCacheManager.getInstance().getOrganization(
                avlLine.getSysVendorOrganizationId());
              if (orgRow != null) {
                //If yes, then check if the item exists,
                ItemRow vendorItem = OMSUtil.getItem(
                  new ItemKey(order.getValueChainId(), orgRow.getEntName(), avlLineMDFs.getSupplierItemName()));
                if (vendorItem != null) { //If yes, then populate vendorItem info on line,
                  line.setVendorItemEnterpriseName(vendorItem.getEntName());
                  line.setVendorItemName(vendorItem.getItemName());
                  isVendorItemSet = true;
                }
              }
              if (!isVendorItemSet) { // if vendorItem/supplierItemName is not found but the vendorItemName is set, then set extVendorItemName
                line.setExtVendorItemName(avlLineMDFs.getSupplierItemName());
              }
            }
            else if (!FieldUtil.isNull(avlLineMDFs.getManufacturerItem())
              && !FieldUtil.isNull(avlLineMDFs.getManufacturerEnt())) { //if vendorItem/supplierItemName is not set in AVL, then check for MfgItem
              ItemRow mfgItem = OMSUtil.getItem(
                new ItemKey(
                  order.getValueChainId(),
                  avlLineMDFs.getManufacturerEnt(),
                  avlLineMDFs.getManufacturerItem()));
              if (mfgItem != null) {
                line.setVendorItemEnterpriseName(avlLineMDFs.getManufacturerEnt());
                line.setVendorItemName(avlLineMDFs.getManufacturerItem());
                isVendorItemSet = true;
              }
              if (!isVendorItemSet) { // if vendorItem is not set, then set extVendorItemName
                line.setExtVendorItemName(avlLineMDFs.getManufacturerItem());
              }
            }
          }
        }
        else if (isPlantPullOrder(ctx, avlLines, order)) {
          if (!isContract(order)) {
            order.setIsVMI(false);
            order.setIsConsignment(false);
          }
        }
      }
      if (!order.isIsConsignment()) {
        PartnerRow partnerRow = PartnerUtil.getPartner(order.getSysVendorId(), ctx);
        if (partnerRow != null && !FieldUtil.isNull(partnerRow.getOmsInvoiceActiveCode())) {
          order.setIsConsignment(
            partnerRow.getOmsInvoiceActiveCode().equals(InvoiceActiveCodeEnum.CONSIGNMENT.intValue()));
        }
      }
      //Populate request fields at DS.
      if (!FieldUtil.isNull(line.getSysItemId()) || order.isIsSpot()) {
        line.getRequestSchedules().stream().flatMap(rs -> rs.getDeliverySchedules().stream()).forEach(ds -> {
          if (FieldUtil.isNull(ds.getRequestUnitPriceAmount())) {
            if (!FieldUtil.isNull(line.getUnitPrice())) {
              ds.setRequestUnitPriceAmount(line.getUnitPrice());
              if (FieldUtil.isNull(line.getCurrency())) {
                if (!FieldUtil.isNull(order.getCurrency())) {
                  line.setCurrency(order.getCurrency());
                }
                else {
                  order.setCurrency(CurrencyCode.USD.toString());
                  line.setCurrency(CurrencyCode.USD.toString());
                }
              }
              ds.setRequestUnitPriceUOM(line.getCurrency());
            }
          }
          DeliveryScheduleMDFs omsDsMdfs = ds.getMDFs(DeliveryScheduleMDFs.class);
          OrderLineMDFs omsLineMdfs = line.getMDFs(OrderLineMDFs.class);
          if (FieldUtil.isNull(omsDsMdfs.getRequestPricePer())) {
            if (!FieldUtil.isNull(omsLineMdfs.getPricePer())) {
              omsDsMdfs.setRequestPricePer(omsLineMdfs.getPricePer());
            }
          }
        });
      }

      //Set AutoReceipt and AutoMoveToReceived values from Vendor master for SpotPO
      if (order.isIsSpot()) {
        order.setIsAutoReceipt(getIsAutoReceipt(order, ctx));
        EnhancedOrderMDFs orderMDFs = order.getMDFs(EnhancedOrderMDFs.class);
        orderMDFs.setAutoMoveToReceived(getIsAutoMoveToReceived(order, ctx));
      }
      populateGenericItem(line, ctx);
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
  }
  
  /**
   * populate generic item 
   *
   * @param line
   * @param ctx
   */
  public static void populateGenericItem(OrderLine line, PlatformUserContext ctx) {
    if(OrderUtil.nonTransitionalStates.contains(line.getState()) ||
        !line.getParent().getOrderType().equals(OrderTypeEnum.PURCHASE_ORDER.toString())) {
      return;
    }
    if(FieldUtil.isNull(line.getExtItemName()) && FieldUtil.isNull(line.getSysGenericItemId())){
      if(!FieldUtil.isNull(line.getSysSpecificItemId())) {
        ItemRow itemRow = ItemCacheManager.getInstance().getItem(line.getSysSpecificItemId());
        if(!FieldUtil.isNull(itemRow.getSccSysGenericItemId())) {
          line.setSysGenericItemId(itemRow.getSccSysGenericItemId(), true);
        } else if(itemRow.isGeneric()) {
          line.setSysGenericItemId(itemRow.getSysItemId(), true);
        }
      } 
      if(FieldUtil.isNull(line.getSysSpecificItemId()) && !FieldUtil.isNull(line.getSysItemId())) {
        ItemRow itemRow = ItemCacheManager.getInstance().getItem(line.getSysItemId());
        if(!FieldUtil.isNull(itemRow.getSccSysGenericItemId())) {
          if(!FieldUtil.isNull(itemRow.getSccSysGenericItemId())) {
            line.setSysGenericItemId(itemRow.getSccSysGenericItemId(), true);
          }
        } else if(itemRow.isGeneric()) {
          line.setSysGenericItemId(itemRow.getSysItemId(), true);
        }
      }
    }
  }
  
  public static void populateBPONumber(EnhancedOrder order, AvlLine avlLine, OrderLine line, Boolean isFirstLine) {
    if (FieldUtil.isNull(order.getBPONumber()) && isFirstLine && !FieldUtil.isNull(avlLine.getBPONumber()))
      order.setBPONumber(avlLine.getBPONumber());
    if (!FieldUtil.isNull(avlLine.getBPONumber()) && FieldUtil.isDifferent(order.getBPONumber(), avlLine.getBPONumber())
      && OrderTypeEnum.PURCHASE_ORDER.stringValue().equalsIgnoreCase(order.getOrderType()) && !order.isIsSpot()
      && !isContract(order)) {
      if (!order.isSetError()) {
        String bpoNumber = "Blank";
        String avlBPONumber = "Blank";

        if (!FieldUtil.isNull(order.getBPONumber()))
          bpoNumber = order.getBPONumber();

        if (!FieldUtil.isNull(avlLine.getBPONumber()))
          avlBPONumber = avlLine.getBPONumber();

        order.setError("OMS.enhancedOrder.BPONumberMismatch", line.getItemName(), avlBPONumber, bpoNumber);
        line.setError("OMS.enhancedOrder.BPONumberMismatch", line.getItemName(), avlBPONumber, bpoNumber);
      }
    }
    if (FieldUtil.isNull(avlLine.getBPOLineNumber())
      ? false
      : (FieldUtil.isNull(line.getBPOLineNumber())
        || (FieldUtil.isDifferent(line.getBPOLineNumber(), avlLine.getBPOLineNumber())))) {
      line.setBPOLineNumber(avlLine.getBPOLineNumber());
    }
  }

  public static boolean populateVendorItemPriceFromMapppedItem(OrderLine orderLine, PlatformUserContext ctx) {
    EnhancedOrder order = orderLine.getParent();
    String collaborativeFields = OrderUtil.getCollaborativeFields(order, ctx);
    if (order.isIsSpot() && collaborativeFields.contains(OrderUpdateAlertFieldsEnum.UNITPRICE.toString())
      && !FieldUtil.isNull(orderLine.getItemEnterpriseName()) && !FieldUtil.isNull(orderLine.getItemName())) {
      ItemKey key = new ItemKey(ctx.getValueChainId(), orderLine.getItemEnterpriseName(), orderLine.getItemName());
      ItemRow itemrow = ItemCacheManager.getInstance().getItem(key);
      if (itemrow != null) {
        SqlParams sqlParams = new SqlParams();
        sqlParams.setLongValue("ITEM_ID", itemrow.getSysItemId());
        ModelRetrieval modelRetrieval = ModelQuery.retrieve(ItemMapping.class);
        modelRetrieval.setIncludeAttachments(ItemMapping.class, false);
        List<ItemMapping> itemMappings = DirectModelAccess.read(
          ItemMapping.class,
          ctx,
          sqlParams,
          ModelQuery.sqlFilter("SYS_ITEM_ID = $ITEM_ID$ "),modelRetrieval);
        List<ItemMapping> mappedItemList = itemMappings.stream().filter(
          mapping -> (mapping.isActive()
            && mapping.getMappedItemEnterpriseName().equals(order.getSellingOrgEnterpriseName()))).collect(
              Collectors.toList());
        if (mappedItemList.size() == 1) {
          return true;
        }
      }
    }
    return false;
  }

  public static boolean populateVendorItemPricePerFromMapppedItem(OrderLine orderLine, PlatformUserContext ctx) {
    EnhancedOrder order = orderLine.getParent();
    String collaborativeFields = OrderUtil.getCollaborativeFields(order, ctx);
    if (order.isIsSpot() && collaborativeFields.contains(OrderUpdateAlertFieldsEnum.PRICEPER.toString())
      && !FieldUtil.isNull(orderLine.getItemEnterpriseName()) && !FieldUtil.isNull(orderLine.getItemName())) {
      ItemKey key = new ItemKey(ctx.getValueChainId(), orderLine.getItemEnterpriseName(), orderLine.getItemName());
      ItemRow itemrow = ItemCacheManager.getInstance().getItem(key);
      SqlParams sqlParams = new SqlParams();
      sqlParams.setLongValue("ITEM_ID", itemrow.getSysItemId());
      ModelRetrieval modelRetrieval = ModelQuery.retrieve(ItemMapping.class);
      modelRetrieval.setIncludeAttachments(ItemMapping.class, false);
      List<ItemMapping> itemMappings = DirectModelAccess.read(
        ItemMapping.class,
        ctx,
        sqlParams,
        ModelQuery.sqlFilter("SYS_ITEM_ID = $ITEM_ID$ "), modelRetrieval);
      List<ItemMapping> mappedItemList = itemMappings.stream().filter(
        mapping -> (mapping.isActive()
          && mapping.getMappedItemEnterpriseName().equals(order.getSellingOrgEnterpriseName()))).collect(
            Collectors.toList());
      if (mappedItemList.size() == 1) {
        return true;
      }
    }
    return false;
  }
  
  /**
   * Checks if Line type is Category
   *
   * @param line
   * @return
   */
  public static boolean isCategoryLine(OrderLine line) {
    return OrderLineTypeEnum.CATEGORY.toString().equals(line.getLineType());
  }

  public static long getValidItem(DeliverySchedule ds) {
    if (!FieldUtil.isNull(ds.getSysAgreedItemId())) {
      return ds.getSysAgreedItemId();
    }
    else if (!FieldUtil.isNull(ds.getSysPromiseItemId())) {
      return ds.getSysPromiseItemId();
    }
    else {
      return 0;
    }
  }

  public static void populateRODefaultLineValues(EnhancedOrder order, PlatformUserContext ctx) {
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled())
        psrKey = PSRLogger.enter(PSR_ID + "populateRODefaultLineValues");
      Long sysShipToLocationId = null;
      for (OrderLine line : order.getOrderLines()) {
        Long itemId = null;
        Long sysShipToSiteId = null;
        if (!FieldUtil.isNull(line.getSysItemId())) {
          itemId = line.getSysItemId();
        }
        for (RequestSchedule rs : line.getRequestSchedules()) {
          if (!FieldUtil.isNull(rs.getSysShipToLocationId())) {
            sysShipToLocationId = rs.getSysShipToLocationId();
          }
          if (!FieldUtil.isNull(rs.getSysShipToSiteId()) && FieldUtil.isNull(sysShipToSiteId)) {
            sysShipToSiteId = rs.getSysShipToSiteId();
          }
          if (sysShipToSiteId != null && !sysShipToSiteId.equals(rs.getSysShipToSiteId())) {
            LOG.warn("Multiple price were present and we picked the first.");
            break;
          }
        }
      }
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
  }

  /**
   * Set Unit Price
   *
   * @param order
   * @param ctx
   * @param sysShipToLocationId
   * @param line
   * @param itemId
   * @param sysShipToSiteId
   */
  private static void setUnitPrice(
    EnhancedOrder order,
    PlatformUserContext ctx,
    Long sysShipToLocationId,
    OrderLine line,
    Long itemId,
    Long sysShipToSiteId) {
	  try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "setUnitPrice")) {
		  boolean acceptUserProvidedUnitPrice = TransactionCache.getOrgPolicy(
				  OrgProcPolicyConstants.ACCEPT_USER_PROVIDED_UNIT_PRICE,
				  order.getSysOwningOrgId(),
				  false,
				  ctx);
		  if(FieldUtil.isNull(line.getCurrency())){
			  if(!FieldUtil.isNull(order.getCurrency())) {
				  line.setCurrency(order.getCurrency());
			  } else {
				  order.setCurrency(CurrencyCode.USD.toString());
				  line.setCurrency(CurrencyCode.USD.toString());
			  }
		  }
		  if (acceptUserProvidedUnitPrice && !FieldUtil.isNull(itemId)) {
			  if (FieldUtil.isNull(line.getUnitPrice())) {
				  line.setUnitPrice(0.0);
			  }
			  if(line.getUnitPrice()<0) {
				  order.setError("Please provide unit price greater than 0");
			  }
		  }
		  else {
			  double price = ((order.isIsSpot() || OrderUtil.isDeploymentOrder(order))
					  ? getUnitPriceForSpotPO(itemId, sysShipToSiteId, sysShipToLocationId, ctx)
							  : getUnitPrice(
									  itemId,
									  order.getSysBuyingOrgId(),
									  order.getSysSellingOrgId(),
									  sysShipToSiteId,
									  order.getSysOwningOrgId(),
									  sysShipToLocationId,
									  line.getSysProductGroupLevelId(),
									  ctx));
			  if (line.getUnitPrice() < DvceConstants.EPSILON
					  && (FieldUtil.isNull(order.getSysRequisitionId()) || order.getSysRequisitionId() <= 0)) {
				  if (price <= 0 && !FieldUtil.isNull(line.getSysGenericItemId())) {
					  price = (order.isIsSpot()
							  ? getUnitPriceForSpotPO(line.getSysGenericItemId(), sysShipToSiteId, sysShipToLocationId, ctx)
									  : getUnitPrice(
											  line.getSysGenericItemId(),
											  order.getSysBuyingOrgId(),
											  order.getSysSellingOrgId(),
											  sysShipToSiteId,
											  order.getSysOwningOrgId(),
											  sysShipToLocationId,
											  ctx));
				  }
				  line.setUnitPrice(price);
			  }
			  String collaborativeFields = OrderUtil.getCollaborativeFields(order, ctx);

			  if (collaborativeFields != null && !collaborativeFields.contains(OmsConstants.COLLABORATIVE_FIELD_UNIT_PRICE)) {
				  for (RequestSchedule rs : line.getRequestSchedules()) {
					  for (DeliverySchedule ds : rs.getDeliverySchedules()) {
						  ds.setRequestUnitPriceAmount(line.getUnitPrice());
						  if(!FieldUtil.isNull(line.getCurrency())) {
							  ds.setRequestUnitPriceUOM(line.getCurrency());
						  } else if(!FieldUtil.isNull(order.getCurrency())){
							  ds.setRequestUnitPriceUOM(order.getCurrency());
						  } else {
							  ds.setRequestUnitPriceUOM(DvceConstants.NULL_STRING_VALUE);
						  }
					  }
				  }
			  }
		  }
	  }
  }

  
  /**
   * Set Price Price
   *
   * @param order
   * @param ctx
   * @param sysShipToLocationId
   * @param line
   * @param itemId
   * @param sysShipToSiteId
   */
  private static void setPricePer(
    EnhancedOrder order,
    PlatformUserContext ctx,
    Long sysShipToLocationId,
    OrderLine line,
    Long itemId,
    Long sysShipToSiteId) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "setPricePer")) {
      // If policy is true then accept as provided by user 
      // If set to false set as per master data
      boolean acceptUserProvidedUnitPrice = TransactionCache.getOrgPolicy(
        OrgProcPolicyConstants.ACCEPT_USER_PROVIDED_UNIT_PRICE,
        order.getSysOwningOrgId(),
        false,
        ctx);
      OrderLineMDFs omsLineMdfs = line.getMDFs(OrderLineMDFs.class);

      // If user accept is set and price is not set then we need to populate value
      // If user accept is not set then we must populate value
      if (!acceptUserProvidedUnitPrice
        || (acceptUserProvidedUnitPrice && (FieldUtil.isNull(omsLineMdfs.getPricePer())
          || omsLineMdfs.getPricePer() < DvceConstants.EPSILON))) {
        double pricePer = ((order.isIsSpot() || OrderUtil.isDeploymentOrder(order))
          ? getPricePerForSpotPO(itemId, sysShipToSiteId, sysShipToLocationId, ctx)
          : getPricePer(
            itemId,
            order.getSysBuyingOrgId(),
            order.getSysSellingOrgId(),
            sysShipToSiteId,
            order.getSysOwningOrgId(),
            sysShipToLocationId,
            line.getSysProductGroupLevelId(),
            ctx));
        if ((!acceptUserProvidedUnitPrice
            || omsLineMdfs.getPricePer() < DvceConstants.EPSILON)
            && (FieldUtil.isNull(order.getSysRequisitionId()) || order.getSysRequisitionId() <= 0)) {
          if (pricePer <= 0 && !FieldUtil.isNull(line.getSysGenericItemId())) {
            pricePer = (order.isIsSpot()
              ? getPricePerForSpotPO(line.getSysGenericItemId(), sysShipToSiteId, sysShipToLocationId, ctx)
              : getPricePer(
                line.getSysGenericItemId(),
                order.getSysBuyingOrgId(),
                order.getSysSellingOrgId(),
                sysShipToSiteId,
                order.getSysOwningOrgId(),
                sysShipToLocationId,
                ctx));
          }
          if (pricePer > 0) {
            omsLineMdfs.setPricePer(pricePer);
          } else if(!acceptUserProvidedUnitPrice && pricePer <= 0) {
            omsLineMdfs.setPricePer(DvceConstants.NULL_DOUBLE_VALUE);
          }
        }
        String collaborativeFields = OrderUtil.getCollaborativeFields(order, ctx);
        if (collaborativeFields != null && !collaborativeFields.contains(OmsConstants.COLLABORATIVE_FIELD_PRICE_PER)) {
          for (RequestSchedule rs : line.getRequestSchedules()) {
            for (DeliverySchedule ds : rs.getDeliverySchedules()) {
              DeliveryScheduleMDFs omsDsMdfs = ds.getMDFs(DeliveryScheduleMDFs.class);
              if (!FieldUtil.isNull(omsLineMdfs.getPricePer()) && omsLineMdfs.getPricePer() > 0) {
                omsDsMdfs.setRequestPricePer(omsLineMdfs.getPricePer());
              }
            }
          }
        }
      }
    }
  }
  
  public static double getUnitPrice(
    Long itemId,
    Long sysBuyingOrgId,
    Long sysSellingOrgId,
    Long sysShipToSiteId,
    Long sysOwningOrgId,
    Long sysShipToLocationId,
    PlatformUserContext ctx) {
    return getUnitPrice(
      itemId,
      sysBuyingOrgId,
      sysSellingOrgId,
      sysShipToSiteId,
      sysOwningOrgId,
      sysShipToLocationId,
      null,
      ctx);
  }
  
  public static double getPricePer(
    Long itemId,
    Long sysBuyingOrgId,
    Long sysSellingOrgId,
    Long sysShipToSiteId,
    Long sysOwningOrgId,
    Long sysShipToLocationId,
    PlatformUserContext ctx) {
    return getPricePer(
      itemId,
      sysBuyingOrgId,
      sysSellingOrgId,
      sysShipToSiteId,
      sysOwningOrgId,
      sysShipToLocationId,
      null,
      ctx);
  }

  public static double getUnitPrice(
    Long itemId,
    Long sysBuyingOrgId,
    Long sysSellingOrgId,
    Long sysShipToSiteId,
    Long sysOwningOrgId,
    Long sysShipToLocationId,
    Long sysCommodityCodeId,
    PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getUnitPrice")) {
      PartnerRow vendorRow = PartnerCacheManager.getInstance().getPartner(
        sysBuyingOrgId,
        sysSellingOrgId,
        OmsConstants.VENDOR_MASTER,
        (DvceContext) ctx);

      if (vendorRow != null) {
        AvlLine avlLine = TransactionCache.getAvlLine(
          itemId,
          sysBuyingOrgId,
          sysSellingOrgId,
          sysShipToSiteId,
          vendorRow.getSysPartnerId(),
          sysCommodityCodeId,
          ctx);
        if (avlLine != null) {
          if (avlLine.isSetUnitPrice() && !FieldUtil.isNull(avlLine.getUnitPrice())) {
            return avlLine.getUnitPrice();
          }
          else {
            avlLine = TransactionCache.getAvlLine(
              itemId,
              sysBuyingOrgId,
              sysSellingOrgId,
              null,
              vendorRow.getSysPartnerId(),
              sysCommodityCodeId,
              ctx);
            if (avlLine != null && avlLine.isSetUnitPrice() && !FieldUtil.isNull(avlLine.getUnitPrice())) {
              return avlLine.getUnitPrice();
            }
          }
        }
      }

      ItemRow item = ItemCacheManager.getInstance().getItem(itemId);
      Buffer buffer = null;
      if (item != null && sysShipToSiteId != null) {
        buffer = TransactionCache.getBuffer(item.getSysItemId(), sysShipToSiteId, sysShipToLocationId, ctx);
      }

      if (buffer != null && OrderUtil.getSCCBufferMdfs(buffer) != null
        && OrderUtil.getSCCBufferMdfs(buffer).isSetStandardCost()
        && !FieldUtil.isNull(OrderUtil.getSCCBufferMdfs(buffer).getStandardCost())) {
        return OrderUtil.getSCCBufferMdfs(buffer).getStandardCost();
      }
      else if (item != null && !FieldUtil.isNull(item.getStandardCost())) {
        return item.getStandardCost();
      }
      else {
        return 0.0;
      }
    }
  }
  
  public static double getPricePer(
    Long itemId,
    Long sysBuyingOrgId,
    Long sysSellingOrgId,
    Long sysShipToSiteId,
    Long sysOwningOrgId,
    Long sysShipToLocationId,
    Long sysCommodityCodeId,
    PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getUnitPrice")) {
      PartnerRow vendorRow = PartnerCacheManager.getInstance().getPartner(
        sysBuyingOrgId,
        sysSellingOrgId,
        OmsConstants.VENDOR_MASTER,
        (DvceContext) ctx);

      if (vendorRow != null) {
        AvlLine avlLine = TransactionCache.getAvlLine(
          itemId,
          sysBuyingOrgId,
          sysSellingOrgId,
          sysShipToSiteId,
          vendorRow.getSysPartnerId(),
          sysCommodityCodeId,
          ctx);
        if (avlLine != null) {
          AvlLineMDFs omsAvlMdfs = avlLine.getMDFs(AvlLineMDFs.class);
          if (omsAvlMdfs.isSetPricePer() && !FieldUtil.isNull(omsAvlMdfs.getPricePer()) && omsAvlMdfs.getPricePer() > 0) {
            return omsAvlMdfs.getPricePer();
          }
          else {
            avlLine = TransactionCache.getAvlLine(
              itemId,
              sysBuyingOrgId,
              sysSellingOrgId,
              null,
              vendorRow.getSysPartnerId(),
              sysCommodityCodeId,
              ctx);
            if (avlLine != null) {
              omsAvlMdfs = avlLine.getMDFs(AvlLineMDFs.class);
              if (omsAvlMdfs.isSetPricePer() && !FieldUtil.isNull(omsAvlMdfs.getPricePer()) && omsAvlMdfs.getPricePer() > 0) {
                return omsAvlMdfs.getPricePer();
              }
            }
          }
        }
      }

      ItemRow item = ItemCacheManager.getInstance().getItem(itemId);
      Buffer buffer = null;
      if (item != null && sysShipToSiteId != null) {
        buffer = TransactionCache.getBuffer(item.getSysItemId(), sysShipToSiteId, sysShipToLocationId, ctx);
      }

      if (buffer != null && OrderUtil.getOMSBufferMdfs(buffer) != null
        && OrderUtil.getOMSBufferMdfs(buffer).isSetPricePer()
        && !FieldUtil.isNull(OrderUtil.getOMSBufferMdfs(buffer).getPricePer())
        && OrderUtil.getOMSBufferMdfs(buffer).getPricePer() > 0) {
        return OrderUtil.getOMSBufferMdfs(buffer).getPricePer();
      }
      else if (item != null && !FieldUtil.isNull(item.getOmsPricePer()) && item.getOmsPricePer() > 0) {
        return item.getOmsPricePer();
      }
      else {
        return 0;
      }
    }
  }

  /**
   * Get UnitPrice information only for Spot PO
   * For Spot PO unit price is fetched from Buffer and Item
   * @param itemId
   * @param sysShipToSiteId
   * @param sysShipToLocationId
   * @param ctx
   * @return
   */
  public static double getUnitPriceForSpotPO(
    Long itemId,
    Long sysShipToSiteId,
    Long sysShipToLocationId,
    PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getUnitPriceForSpotPO")) {

      ItemRow item = ItemCacheManager.getInstance().getItem(itemId);
      Buffer buffer = null;
      if (item != null && sysShipToSiteId != null) {
        buffer = TransactionCache.getBuffer(item.getSysItemId(), sysShipToSiteId, sysShipToLocationId, ctx);
      }
      if (buffer != null && OrderUtil.getSCCBufferMdfs(buffer) != null
        && OrderUtil.getSCCBufferMdfs(buffer).isSetStandardCost()) {
        if(!FieldUtil.isNull(OrderUtil.getSCCBufferMdfs(buffer).getStandardCost())) {
          return OrderUtil.getSCCBufferMdfs(buffer).getStandardCost();
        }else if(!FieldUtil.isNull(sysShipToLocationId) && sysShipToLocationId != -1) {
          buffer = TransactionCache.getBuffer(item.getSysItemId(), sysShipToSiteId, null, ctx);
          if(buffer != null && OrderUtil.getSCCBufferMdfs(buffer) != null
            && OrderUtil.getSCCBufferMdfs(buffer).isSetStandardCost()) {
            return OrderUtil.getSCCBufferMdfs(buffer).getStandardCost();
          }
        }
      }else if(!FieldUtil.isNull(sysShipToLocationId) && sysShipToLocationId != -1) {
        buffer = TransactionCache.getBuffer(item.getSysItemId(), sysShipToSiteId, null, ctx);
        if(buffer != null && OrderUtil.getSCCBufferMdfs(buffer) != null
          && OrderUtil.getSCCBufferMdfs(buffer).isSetStandardCost()) {
          return OrderUtil.getSCCBufferMdfs(buffer).getStandardCost();
        }
      }
      if (item != null && !FieldUtil.isNull(item.getStandardCost())) {
        return item.getStandardCost();
      }
      else {
        return 0.0;
      }
    }
  }
  
  /**
   * Get PricePer information only for Spot PO
   * For Spot PO pricePer is fetched from Buffer and Item
   * @param itemId
   * @param sysShipToSiteId
   * @param sysShipToLocationId
   * @param ctx
   * @return
   */
  public static double getPricePerForSpotPO(
    Long itemId,
    Long sysShipToSiteId,
    Long sysShipToLocationId,
    PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getPricePerForSpotPO")) {

      ItemRow item = ItemCacheManager.getInstance().getItem(itemId);
      Buffer buffer = null;
      if (item != null && sysShipToSiteId != null) {
        buffer = TransactionCache.getBuffer(item.getSysItemId(), sysShipToSiteId, sysShipToLocationId, ctx);
      }

      if (buffer != null && OrderUtil.getOMSBufferMdfs(buffer) != null
        && OrderUtil.getOMSBufferMdfs(buffer).isSetPricePer()
        && !FieldUtil.isNull(OrderUtil.getOMSBufferMdfs(buffer).getPricePer())) {
        return OrderUtil.getOMSBufferMdfs(buffer).getPricePer();
      }
      else if (item != null && !FieldUtil.isNull(item.getOmsPricePer())) {
        return item.getOmsPricePer();
      }
      else {
        return 0;
      }
    }
  }
  
  /**
   * Get PricePer information only for Spot PO
   * For Spot PO pricePer is fetched from Buffer and Item
   * @param itemId
   * @param sysShipToSiteId
   * @param sysShipToLocationId
   * @param ctx
   * @return
   */
  public static Double getPricePerForDO(
    Long itemId,
    Long sysShipToSiteId,
    Long sysShipToLocationId,
    PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getPricePerForDO")) {
      Double pricePer = 0d;
      ItemRow item = ItemCacheManager.getInstance().getItem(itemId);
      Buffer buffer = null;
      if (item != null && sysShipToSiteId != null) {
        buffer = TransactionCache.getBuffer(item.getSysItemId(), sysShipToSiteId, sysShipToLocationId, ctx);
      }

      if (buffer != null && OrderUtil.getOMSBufferMdfs(buffer) != null
        && OrderUtil.getOMSBufferMdfs(buffer).isSetPricePer()
        && !FieldUtil.isNull(OrderUtil.getOMSBufferMdfs(buffer).getPricePer())) {
        pricePer = OrderUtil.getOMSBufferMdfs(buffer).getPricePer();
      }
      else if (item != null && !FieldUtil.isNull(item.getOmsPricePer())) {
        pricePer =  item.getOmsPricePer();
      }
      if(!FieldUtil.isNull(pricePer) && pricePer>0d) {
        return pricePer;
      }
      else {
        return null;
      }
    }
  }

  public static boolean isPlantPullOrder(PlatformUserContext ctx, Map<RequestSchedule, AvlLine> avlLines, EnhancedOrder order) {
    final String TRANSIENT_KEY = "OMS_isPlantPullOrder";
    Boolean cachedValue = (Boolean) order.getTransientField(TRANSIENT_KEY);
    if (cachedValue != null)
      return cachedValue;
    Long storageSiteValue = OrderUtil.getStorageSiteValue(order, avlLines, ctx);
    if (!FieldUtil.isNull(storageSiteValue)) {
      for (OrderLine line : order.getOrderLines()) {
        for (RequestSchedule rs : line.getRequestSchedules()) {
          if (!rs.getSysShipToSiteId().equals(storageSiteValue)) {
            order.setTransientField(TRANSIENT_KEY, true);
            return true;
          }
        }
      }
    }
    order.setTransientField(TRANSIENT_KEY, false);
    return false;
  }

  public static void populateAgreedShipDate(EnhancedOrder order, PlatformUserContext ctx) {
    for (OrderLine line : order.getOrderLines()) {
      for (RequestSchedule rs : line.getRequestSchedules()) {
        for (DeliverySchedule ds : rs.getDeliverySchedules()) {
          if (States.OPEN.equalsIgnoreCase(ds.getState())) {
            if (!FieldUtil.isNull(ds.getPromiseShipDate())) {
              ds.setAgreedShipDate(ds.getPromiseShipDate());
            }
          }
        }
      }
    }
  }

  public static void clearPromiseFields(EnhancedOrder order, String state) {
    for (OrderLine line : order.getOrderLines()) {
      for (RequestSchedule rs : line.getRequestSchedules()) {
        for (DeliverySchedule ds : rs.getDeliverySchedules()) {
          clearPromiseFields(ds, state);
        }
      }
    }
  }

  public static void clearAgreedFields(EnhancedOrder order, String state) {
    for (OrderLine line : order.getOrderLines()) {
      for (RequestSchedule rs : line.getRequestSchedules()) {
        for (DeliverySchedule ds : rs.getDeliverySchedules()) {
          clearAgreedFields(ds, state);
        }
      }
    }
  }

  public static int compareDate(Date date1, Date date2, boolean ignoreTime) {
    if (ignoreTime) {
      DateTimeComparator dateTimeComparator = DateTimeComparator.getDateOnlyInstance();
      int retVal = dateTimeComparator.compare(date1, date2);
      if (retVal == 0)
        //both dates are equal
        return 0;
      else if (retVal < 0)
        //myDateOne is before myDateTwo
        return -1;
      else if (retVal > 0)
        //myDateOne is after myDateTwo
        return 1;
      return 0;
    }
    else {
      if (date1.equals(date2)) {
        return 0;
      }
      else if (date1.before(date2))
        //myDateOne is before myDateTwo
        return -1;
      else if (date1.after(date2))
        //myDateOne is after myDateTwo
        return 1;
      return 0;
    }

  }
  public static void clearDeviationreasonCode(EnhancedOrder order, String state) {
    for(OrderLine line : order.getOrderLines()) {
      for(RequestSchedule rs : line.getRequestSchedules()) {
        for(DeliverySchedule ds : rs.getDeliverySchedules()) {
          if(state.equalsIgnoreCase(ds.getState())) {
            ds.getMDFs(DeliveryScheduleMDFs.class).setDeviationReasonCode(DvceConstants.NULL_STRING_VALUE);
            ds.getMDFs(DeliveryScheduleMDFs.class).setVendorDeviationComment(DvceConstants.NULL_STRING_VALUE);
          }
        }
      }
    }
  }

  public static void clearBackOrderReasonCode(EnhancedOrder order, String state) {
    for(OrderLine line : order.getOrderLines()) {
      for(RequestSchedule rs : line.getRequestSchedules()) {
        for(DeliverySchedule ds : rs.getDeliverySchedules()) {
          if(state.equalsIgnoreCase(ds.getState())) {
            ds.getMDFs(DeliveryScheduleMDFs.class).setBackOrderReasonCode(DvceConstants.NULL_STRING_VALUE);
          }
        }
      }
    }
  }

  public static void clearPromiseFields(DeliverySchedule ds, String state) {
    if (Objects.isNull(state) || state.equals(ds.getState())) {
      DeliveryScheduleMDFs omsDsMdfs = ds.getMDFs(DeliveryScheduleMDFs.class);
      ds.setPromiseDeliveryDate(DvceConstants.NULL_CALENDAR_VALUE);
      ds.setPromiseQuantity(DvceConstants.NULL_DOUBLE_VALUE);
      ds.setPromiseShipDate(DvceConstants.NULL_CALENDAR_VALUE);
      ds.setSysPromiseItemId(DvceConstants.NULL_LONG_VALUE, true);
      ds.setPromiseUnitPriceAmount(DvceConstants.NULL_DOUBLE_VALUE);
      ds.setPromiseUnitPriceUOM(DvceConstants.NULL_STRING_VALUE);
      ds.setPromiseIncoDateStartDate(DvceConstants.NULL_CALENDAR_VALUE);
      ds.setPromiseIncoDateEndDate(DvceConstants.NULL_CALENDAR_VALUE);
      ds.setPromiseMinItemExpiryDate(DvceConstants.NULL_CALENDAR_VALUE);
      omsDsMdfs.setPromisePricePer(DvceConstants.NULL_DOUBLE_VALUE);
    }
  }

  public static void clearAgreedFields(DeliverySchedule ds, String state) {
    if (Objects.isNull(state) || state.equalsIgnoreCase(ds.getState())) {
      DeliveryScheduleMDFs omsDsMdfs = ds.getMDFs(DeliveryScheduleMDFs.class);
      ds.setAgreedDeliveryDate(DvceConstants.NULL_CALENDAR_VALUE);
      ds.setAgreedQuantity(DvceConstants.NULL_DOUBLE_VALUE);
      ds.setAgreedShipDate(DvceConstants.NULL_CALENDAR_VALUE);
      ds.setSysAgreedItemId(DvceConstants.NULL_LONG_VALUE, true);
      ds.setAgreedUnitPriceAmount(DvceConstants.NULL_DOUBLE_VALUE);
      ds.setAgreedUnitPriceUOM(DvceConstants.NULL_STRING_VALUE);
      ds.setAgreedIncoDateStartDate(DvceConstants.NULL_CALENDAR_VALUE);
      ds.setAgreedIncoDateEndDate(DvceConstants.NULL_CALENDAR_VALUE);
      ds.setAgreedMinItemExpiryDate(DvceConstants.NULL_CALENDAR_VALUE);
      omsDsMdfs.setAgreedPricePer(DvceConstants.NULL_DOUBLE_VALUE);
    }
  }

  public static boolean isTriggerShipmentCancel(EnhancedOrder order, PlatformUserContext context) {
    boolean isTriggerShipmentCancel = false;
    PartnerRow row = PartnerUtil.getPartner(order.getSysVendorId(), null);
    if (row != null && row.getOmsTriggerShipmentCancel() != null && row.getOmsTriggerShipmentCancel() == 1) {
      isTriggerShipmentCancel = true;
    }
    if (isTriggerShipmentCancel == false) {
      isTriggerShipmentCancel = OrgProcPolicyUtil.getBooleanValue(
        (DvceContext) context,
        ((DvceContext) context).getUserContext().getValueChainId(),
        order.getSysOwningOrgId(),
        OrgProcPolicyConstants.TRIGGER_SHIPMENT_CANCEL_BY_ORDER,
        false);
    }
    return isTriggerShipmentCancel;
  }

  public static void setStateForNewChildLevels(EnhancedOrder inputOrder, EnhancedOrder currentOrder) {

    if (Objects.isNull(currentOrder))
      return;

    List<OrderLine> newLines = getNewRecords(inputOrder.getOrderLines(), currentOrder.getOrderLines(), "OrderLine");
    if (null != newLines && !newLines.isEmpty()) {
      setupLinesState(newLines, inputOrder.getState());
    }
    for (OrderLine line : inputOrder.getOrderLines()) {
      OrderLine currentLine = findMatchingOrderLine(currentOrder, line);
      if (null != currentLine) {
        List<RequestSchedule> newReqSchedules = getNewRecords(
          line.getRequestSchedules(),
          currentLine.getRequestSchedules(),
          "RequestSchedule");
        if (null != newReqSchedules && !newReqSchedules.isEmpty()) {
          setupRequestScheduleState(newReqSchedules, line.getState());
        }
      }
    }

    for (OrderLine line : inputOrder.getOrderLines()) {
      for (RequestSchedule reqSchd : line.getRequestSchedules()) {
        RequestSchedule currentReqSchd = findMatchingRequestSchedule(currentOrder, reqSchd);
        if (null != currentReqSchd) {
          List<DeliverySchedule> newDelSchedules = getNewRecords(
            reqSchd.getDeliverySchedules(),
            currentReqSchd.getDeliverySchedules(),
            "DeliverySchedule");
          if (null != newDelSchedules && !newDelSchedules.isEmpty()) {
            setupDeliveryScheduleState(newDelSchedules, reqSchd.getState());
          }
        }
      }
    }
  }

  public static void setStateForNewChildLevels(
    EnhancedOrder inputOrder,
    EnhancedOrder currentOrder,
    String targetState) {
    List<OrderLine> newLines = getNewRecords(inputOrder.getOrderLines(), currentOrder.getOrderLines(), "OrderLine");
    if (null != newLines && !newLines.isEmpty()) {
      setupLinesState(newLines, targetState);
    }
    for (OrderLine line : inputOrder.getOrderLines()) {
      OrderLine currentLine = findMatchingOrderLine(currentOrder, line);
      if (null != currentLine) {
        List<RequestSchedule> newReqSchedules = getNewRecords(
          line.getRequestSchedules(),
          currentLine.getRequestSchedules(),
          "RequestSchedule");
        if (null != newReqSchedules && !newReqSchedules.isEmpty()) {
          setupRequestScheduleState(newReqSchedules, targetState);
        }
      }

      for (RequestSchedule reqSchd : line.getRequestSchedules()) {
        RequestSchedule currentReqSchd = findMatchingRequestSchedule(currentOrder, reqSchd);
        if (null != currentReqSchd) {
          List<DeliverySchedule> newDelSchedules = getNewRecords(
            reqSchd.getDeliverySchedules(),
            currentReqSchd.getDeliverySchedules(),
            "DeliverySchedule");
          if (null != newDelSchedules && !newDelSchedules.isEmpty()) {
            setupDeliveryScheduleState(newDelSchedules, targetState);
          }
        }
        reqSchd.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(reqSchd)));
      }
      line.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(line)));
    }
    List<String> lineChidrenStateList = OrderUtil.getChildrenStateList(inputOrder);
    String orderState = OrderUtil.getEffectiveState(lineChidrenStateList);
    targetState = OrderUtil.getEffectiveOrderHeaderState(lineChidrenStateList, orderState);
    inputOrder.setState(orderState);
  }

  public static void copyRequestFieldsFromSibling(RequestSchedule rs, PlatformUserContext ctx) {
    Calendar reqDelDate = null;
    double reqQty = 0.0;
    double reqUnitPrice = 0.0;
    double reqPricePer = 0.0;
    String priceUOM = "";
    EnhancedOrder order = rs.getParent().getParent();
    for (DeliverySchedule ds : rs.getDeliverySchedules()) {
      DeliveryScheduleMDFs omsDsMdfs = ds.getMDFs(DeliveryScheduleMDFs.class);
      if (reqDelDate == null && !FieldUtil.isNull(ds.getRequestDeliveryDate())
        && !OrderUtil.nonCollaborationStates.contains(ds.getState())) {
        reqDelDate = ds.getRequestDeliveryDate();
      }
      if (reqDelDate != null && FieldUtil.isNull(ds.getRequestDeliveryDate())) {
        ds.setRequestDeliveryDate(reqDelDate);
      }

      if (ds.getRequestQuantity() > DvceConstants.EPSILON
        && !OrderUtil.nonCollaborationStates.contains(ds.getState())) {
        reqQty = ds.getRequestQuantity();
      }
      if (FieldUtil.isNull(ds.getSysId()) && !isSalesOrder(order) && !order.isIsVMI()
        && AbstractOrderCollaboration.isCollabPerDS(ds, (DvceContext) ctx)
        && !(order.getOrigin().equals(OriginEnum.UIUPLOAD.stringValue()) || 
          order.getOrigin().equalsIgnoreCase(OriginEnum.INTEG.stringValue()) || 
          order.getOrigin().equalsIgnoreCase(OriginEnum.EDI.stringValue()))) {
    	  if (ds.isSetRequestQuantity() && reqQty != 0) {
    		  ds.setRequestQuantity(reqQty);
    	  } else {
    		  ds.setRequestQuantity(0);
    	  }
      }
      else if (!ds.isSetRequestQuantity() && reqQty != 0) {
        ds.setRequestQuantity(reqQty);
      }

      if (reqUnitPrice == 0.0 && ds.isSetRequestUnitPriceAmount()
        && !OrderUtil.nonCollaborationStates.contains(ds.getState())) {
        reqUnitPrice = ds.getRequestUnitPriceAmount();
        priceUOM = ds.getRequestUnitPriceUOM();
      }
      if (reqUnitPrice != 0.0 && !ds.isSetRequestUnitPriceAmount()) {
        ds.setRequestUnitPriceAmount(reqUnitPrice);
        ds.setRequestUnitPriceUOM(priceUOM);
      }
      if (reqPricePer == 0.0 && !FieldUtil.isNull(omsDsMdfs.getRequestPricePer())
        && !OrderUtil.nonCollaborationStates.contains(ds.getState())) {
        reqPricePer = omsDsMdfs.getRequestPricePer();
      }
      if (reqPricePer != 0.0 && !omsDsMdfs.isSetRequestPricePer()) {
        omsDsMdfs.setRequestPricePer(reqPricePer);
      }
    }
  }

  public static void checkOrderLineFields(DeliverySchedule ds, PlatformUserContext ctx) {
    RequestSchedule rs = ds.getParent();
    OrderLine line = rs.getParent();
    EnhancedOrder order = line.getParent();
    if (FieldUtil.isNull(ds.getQuantityUOM())) {
      if (!FieldUtil.isNull(line.getQuantityUOM()))
        ds.setQuantityUOM(line.getQuantityUOM());
      else {
        ds.setQuantityUOM(
          getQuantityUOM(line.getSysItemId(), rs.getSysShipToSiteId(), rs.getSysShipToLocationId(), ctx));
      }
    }
    if (order.isIsVMI()) {
      ds.setOrigPromiseDeliveryDate(ds.getPromiseDeliveryDate());
      if (FieldUtil.isNull(ds.getRequestDeliveryDate())) {
        ds.setRequestDeliveryDate(ds.getPromiseDeliveryDate());
      }
      ds.setOrigPromisedQuantity(ds.getPromiseQuantity());
      ds.setOrigPromiseShipDate(ds.getPromiseShipDate());
    }
    boolean isSalesOrder = OrderUtil.isSalesOrder(order);
    boolean isParentSkipValidations = Objects.nonNull(order.getTransientField("SkipValidations"))
      ? Boolean.getBoolean((String) order.getTransientField("SkipValidations")) : false;
    if (!(isSalesOrder 
          && !FieldUtil.isNull(rs.getState())
          && (rs.getState().equals(com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.IN_PROMISING))
          && !isParentSkipValidations)) {
      rs.setOrigRequestDeliveryDate(ds.getRequestDeliveryDate());
      rs.setOrigRequestQuantity(ds.getRequestQuantity());
      if(FieldUtil.isNull(rs.getOrigRequestShipDate()) || isSalesOrder) {
        rs.setOrigRequestShipDate(ds.getRequestShipDate());  
      }
      rs.setOriginalRequestQuantityUOM(ds.getQuantityUOM());
    }
    if (FieldUtil.isNull(line.getQuantityUOM()))
      line.setQuantityUOM(rs.getDeliverySchedules().get(0).getQuantityUOM());
    if (!line.isSetLineType()) {
      line.setLineType(OrderLineTypeEnum.PRODUCT.stringValue());
    }
    if (!FieldUtil.isNull(rs.getState()) && (FieldUtil.isNull(rs.getOriginalRequestQuantityUOM())
      || rs.getState().equals(com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.DRAFT)
      || rs.getState().equals(States.AWAITING_APPROVAL) || (isSalesOrder && !isParentSkipValidations && rs.getState().equals(States.NEW)))) {
      rs.setOriginalRequestQuantityUOM(ds.getQuantityUOM());
    }
  }

  public static void setOriginalRequestFields(DeliverySchedule ds) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "setOriginalRequestFields")) {
    RequestSchedule rs = ds.getParent();
    rs.setOrigRequestDeliveryDate(ds.getRequestDeliveryDate());
    rs.setOrigRequestQuantity(ds.getRequestQuantity());
    if(FieldUtil.isNull(rs.getOrigRequestShipDate())) {
      rs.setOrigRequestShipDate(ds.getRequestShipDate());  
    }
    rs.setOriginalRequestQuantityUOM(ds.getQuantityUOM());
    }
  }

  public static void populateCodes(List<EnhancedOrder> orders, PlatformUserContext ctx, boolean create) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "populateCodes")) {
      if (orders.isEmpty())
        return;
      String bpCodeUser = "";
      if (create && orders != null && !FieldUtil.isNull(orders.get(0).getBuyingOrgEnterpriseName())
        && !FieldUtil.isNull(ctx.getRoleEnterpriseName())
        && orders.get(0).getBuyingOrgEnterpriseName().equalsIgnoreCase(ctx.getRoleEnterpriseName())) {
        bpCodeUser = CpDBUtil.getBuyerPlannerCode(ctx.getUserId());
      }

      for (EnhancedOrder order : orders) {
        String bpCode = bpCodeUser;
        String bCode = null;
        String csrCode = null;
        String transMgrCode = null;
        /**** TODO: Get buyer code from contract */
        OrderLine line = null;
        if (order.getOrderLines().size() > 0) {
          line = order.getOrderLines().get(0);
        }
        if (line != null) {
          String itemName = line.getItemName();
          if (!FieldUtil.isNull(itemName) && !itemName.equals(DvceConstants.EMPTY_STRING)) {
            ItemRow item = OMSUtil.getItem(
              new ItemKey(order.getValueChainId(), line.getItemEnterpriseName(), itemName));
            if (item != null) {
              if (bCode == null)
                bCode = item.getOmsBuyerCode();

              for (RequestSchedule rs : line.getRequestSchedules()) {
                AvlLine avlLine = OrderUtil.getAVLFromRS(rs, ctx);
                if (avlLine != null) {
                  AvlLineMDFs avlLineMDFs = avlLine.getMDFs(AvlLineMDFs.class);
                  if (FieldUtil.isNull(csrCode))
                    csrCode = avlLineMDFs.getSupplierCSRCode();

                  if (FieldUtil.isNull(transMgrCode))
                    transMgrCode = avlLineMDFs.getSupplierShipMGRCode();

                  if (!FieldUtil.isNull(csrCode) && !FieldUtil.isNull(transMgrCode))
                    break;
                }
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("csrCode: " + csrCode + ", transMgrCode: " + transMgrCode);
              }

              String origin = order.getOrigin();
              if (create && null != origin
                && (OriginEnum.IXM.stringValue().equalsIgnoreCase(origin) || IXM_LOAD_BUILDER.equals(origin)
                  || OriginEnum.UIUPLOAD.stringValue().equalsIgnoreCase(origin)
                  || OmsConstants.INTEG_ORIGIN.equalsIgnoreCase(origin)
                  || (OriginEnum.UI.stringValue().equalsIgnoreCase(origin)))) {
                bpCode = getPlannerCodeFromMasterData(line, item, ctx);
              }
            }
          }
        }
        if (!FieldUtil.isNull(bCode))
          order.setBuyerCode(bCode);
        if (create && FieldUtil.isNull(order.getPlannerCode())) {
          if (!FieldUtil.isNull(bpCode))
            order.setPlannerCode(bpCode);
          else if (!FieldUtil.isNull(bpCodeUser))
            order.setPlannerCode(bpCodeUser);
        }

        if (!FieldUtil.isNull(csrCode))
          order.setVendorCsrcode(csrCode);
      }
    }
  }

  public static void setDeliveryScheduleStateAndAgreedValues(List<DeliverySchedule> delSchedules, String targetState) {
    for (DeliverySchedule delSchedule : delSchedules) {
      setDeliveryScheduleStateAndAgreedValues(delSchedule, targetState);
    }
  }

  public static void setDeliveryScheduleStateAndAgreedValues(DeliverySchedule delSchedule, String targetState) {
    DeliveryScheduleMDFs omsDsMdfs = delSchedule.getMDFs(DeliveryScheduleMDFs.class);
    String originalRequestState = delSchedule.getState();
    if ((!OrderUtil.nonTransitionalStates.contains(originalRequestState)
      && !OrderUtil.inFullFillmentStates.contains(originalRequestState)) || FieldUtil.isNull(originalRequestState)) {
      delSchedule.setState(targetState);
    }
    if (delSchedule.getState().equals(States.OPEN)) {
      delSchedule.setAgreedQuantity(delSchedule.getPromiseQuantity());
      delSchedule.setAgreedDeliveryDate(delSchedule.getPromiseDeliveryDate());
      delSchedule.setAgreedShipDate(delSchedule.getPromiseShipDate());
      delSchedule.setAgreedItemName(delSchedule.getPromiseItemName());
      delSchedule.setAgreedItemEnterpriseName(delSchedule.getPromiseItemEnterpriseName());
      delSchedule.setSysAgreedItemId(delSchedule.getSysPromiseItemId(), false);
      if (delSchedule.isSetPromiseUnitPriceAmount() && !FieldUtil.isNull(delSchedule.getPromiseUnitPriceAmount())) {
        delSchedule.setAgreedUnitPriceAmount(delSchedule.getPromiseUnitPriceAmount());
      }
      if (!FieldUtil.isNull(omsDsMdfs.getPromisePricePer()) && omsDsMdfs.getPromisePricePer() > DvceConstants.EPSILON) {
        if(omsDsMdfs.getPromisePricePer() > 0) {
          omsDsMdfs.setAgreedPricePer(omsDsMdfs.getPromisePricePer());
        }
      }
      delSchedule.setAgreedUnitPriceUOM(delSchedule.getPromiseUnitPriceUOM());
      delSchedule.setAgreedIncoDateStartDate(delSchedule.getPromiseIncoDateStartDate());
      delSchedule.setAgreedIncoDateEndDate(delSchedule.getPromiseIncoDateEndDate());
      delSchedule.setAgreedMinItemExpiryDate(delSchedule.getPromiseMinItemExpiryDate());
    }
  }

  public static void copyPromiseToAgreed(DeliverySchedule delSch) {
    DeliveryScheduleMDFs omsDsMdfs = delSch.getMDFs(DeliveryScheduleMDFs.class);
    if (!delSch.isSetAgreedDeliveryDate() || FieldUtil.isNull(delSch.getAgreedDeliveryDate())) {
      delSch.setAgreedDeliveryDate(delSch.getPromiseDeliveryDate());
    }
    if (!delSch.isSetAgreedShipDate() || FieldUtil.isNull(delSch.getAgreedShipDate())) {
      delSch.setAgreedShipDate(delSch.getPromiseShipDate());
    }
    if (!delSch.isSetAgreedQuantity() || delSch.getAgreedQuantity() == 0.0 || FieldUtil.isNull(delSch.getAgreedQuantity())) {
      delSch.setAgreedQuantity(delSch.getPromiseQuantity());
    }
    if (FieldUtil.isNull(delSch.getSysAgreedItemId()) || FieldUtil.isNull(delSch.getSysAgreedItemId())) {
      delSch.setSysAgreedItemId(delSch.getSysPromiseItemId(), true);
      delSch.setAgreedItemName(delSch.getPromiseItemName());
      delSch.setAgreedItemEnterpriseName(delSch.getPromiseItemEnterpriseName());
    }
    if (!delSch.isSetPromiseUnitPriceAmount() || FieldUtil.isNull(delSch.getAgreedUnitPriceAmount())) {
      delSch.setAgreedUnitPriceAmount(delSch.getPromiseUnitPriceAmount());
    }
    if (!delSch.isSetAgreedUnitPriceUOM() || FieldUtil.isNull(delSch.getAgreedUnitPriceUOM())) {
      delSch.setAgreedUnitPriceUOM(delSch.getPromiseUnitPriceUOM());
    }
    if (FieldUtil.isNull(delSch.getAgreedIncoDateStartDate()) || FieldUtil.isNull(delSch.getAgreedIncoDateStartDate())) {
      delSch.setAgreedIncoDateStartDate(delSch.getPromiseIncoDateStartDate());
    }
    if (FieldUtil.isNull(delSch.getAgreedIncoDateEndDate()) || FieldUtil.isNull(delSch.getAgreedIncoDateEndDate())) {
      delSch.setAgreedIncoDateEndDate(delSch.getPromiseIncoDateEndDate());
    }
    if (!delSch.isSetAgreedMinItemExpiryDate() || FieldUtil.isNull(delSch.getAgreedMinItemExpiryDate())) {
      delSch.setAgreedMinItemExpiryDate(delSch.getPromiseMinItemExpiryDate());
    }
    if ((!FieldUtil.isNull(omsDsMdfs.getPromisePricePer()) && omsDsMdfs.getPromisePricePer() > DvceConstants.EPSILON)
    		|| FieldUtil.isNull(omsDsMdfs.getAgreedPricePer())) {
      if(omsDsMdfs.getPromisePricePer() > 0 ) {
        omsDsMdfs.setAgreedPricePer(omsDsMdfs.getPromisePricePer());
      }
    }
  }

  /**
   * 
   * To set BackOrder Quantity in Delivery schedule to zero if Request quantity is not equals to BackOrder Quantity.
   *
   * @param order
   */
  public static void setBackOrderQuantityDS(final EnhancedOrder order) {
    for (final OrderLine line : order.getOrderLines()) {
      for (final RequestSchedule rs : line.getRequestSchedules()) {
        for (final DeliverySchedule deliverySchedule : rs.getDeliverySchedules()) {
          if (deliverySchedule.getBackOrderQuantity() > DvceConstants.EPSILON
            && deliverySchedule.getPromiseQuantity() > DvceConstants.EPSILON
            && deliverySchedule.getBackOrderQuantity() != deliverySchedule.getRequestQuantity()) {
            deliverySchedule.setBackOrderQuantity(DvceConstants.NULL_DOUBLE_VALUE);
            //deliverySchedule.unsetBackOrderQuantity();
          }
        }
      }
    }
  }

  public static void updateDeliveryMode(List<EnhancedOrder> orders) {
    if (orders != null) {
      currentOrder: for (EnhancedOrder order : orders) {
        for (OrderLine line : order.getOrderLines()) {
          if (line.getRequestSchedules().size() > 1) {
            order.setDeliveryMode(DeliveryModeEnum.MULTI_DELIVERY.stringValue());
            continue currentOrder;
          }

          for (RequestSchedule rs : line.getRequestSchedules())
            if (rs.getDeliverySchedules().size() > 1)
              order.setDeliveryMode(DeliveryModeEnum.MULTI_DELIVERY.stringValue());
          continue currentOrder;
        }
        order.setDeliveryMode(DeliveryModeEnum.SINGLE_DELIVERY.stringValue());
      }
    }
  }

  /**
   * @see com.ordermgmtsystem.oms.domain.OrderDomain#setReceivedQtySameAsShippedQty(java.util.List)
   */
  public static void setReceivedQtySameAsShippedQty(List<EnhancedOrder> orders) {
    for (EnhancedOrder order : orders) {
      for (OrderLine line : order.getOrderLines()) {
        for (RequestSchedule reqSch : line.getRequestSchedules()) {
          for (DeliverySchedule delSch : reqSch.getDeliverySchedules()) {
            delSch.setReceivedQuantity(delSch.getShippedQuantity());
          }
        }
      }
    }
  }

  /**
   * @see com.ordermgmtsystem.oms.domain.OrderDomain#setShippedQtySameAsRequestQty(java.util.List)
   */
  public static void setShippedQtySameAsRequestQty(List<EnhancedOrder> orders) {
    for (EnhancedOrder order : orders) {
      for (OrderLine line : order.getOrderLines()) {
        for (RequestSchedule reqSch : line.getRequestSchedules()) {
          for (DeliverySchedule delSch : reqSch.getDeliverySchedules()) {
            delSch.setShippedQuantity(getValidQty(delSch));
          }
        }
      }
    }
  }

  /**
   * Identify if PO needs to be automatically acknowledged.
   * If enabled on Partner, then use that value.
   * Else, look at value on AVL Line 
   * @param order EnhancedOrder
   * @param ctx PlatformUserContext
   * @return boolean true, if flag is enabled.
   */
  public static AutoPoAckModeEnum getAutoPoAckMode(EnhancedOrder order, PlatformUserContext ctx) {
    return TransactionCache.getAutoPoAckMode(order, ctx);
  }

  public static Pair<BuyerAcceptanceEnum, AutoPoAckModeEnum> getAcceptanceModes(
    EnhancedOrder order,
    PlatformUserContext ctx) {
    return TransactionCache.getAcceptanceModes(order, ctx);
  }

  /**
   * Identify if DO needs to be automatically acknowledged(Promised).
   *
   * @param order
   * @param ctx
   * @return AutoPOAckMode Enum value
   */
  public static String getShipperOrderAcceptance(EnhancedOrder order, PlatformUserContext ctx) {
    return getOrderSitePolicy(order, Policy.SHIPPER_ORDER_ACCEPTANCE, ctx);
  }

  /**
   * Identify if DO needs to be automatically approved.
   *
   * @param order
   * @param ctx
   * @return BuyerAcceptance Enum value
   */
  public static String getConsigneeOrderAcceptance(EnhancedOrder order, PlatformUserContext ctx) {
    return getOrderSitePolicy(order, Policy.CONSIGNEE_ORDER_ACCEPTANCE, ctx);
  }

  /**
   * Identify if PO needs to be automatically closed.
   * If enabled on Partner, then use that value.
   * Else, look at value on AVL Line 
   * @param order EnhancedOrder
   * @param ctx PlatformUserContext
   * @return boolean true, if flag is enabled.
   */
  public static boolean isAutoCloseOnReceipt(EnhancedOrder order, PlatformUserContext ctx) {
    return TransactionCache.isAutoCloseOnReceipt(order, ctx);
  }

  // Used by TransactionCache to do lookup on isAutoCloseOnReceipt
  public static boolean getIsAutoCloseOnReceipt(EnhancedOrder order, PlatformUserContext ctx) {
 // Initial flag lookup option: Lookup Customer/Vendor Master before going to ACL/AVL lookup (fallback)
    PartnerRow partnerRow = null;
    if(OrderTypeEnum.SALES_ORDER.toString().equals(order.getOrderType())) {
      partnerRow = PartnerUtil.getPartner(order.getSysCustomerId(), ctx); // Customer Master for SOs
    } else {
      partnerRow = PartnerUtil.getPartner(order.getSysVendorId(), ctx); // Vendor Master 
    }
    if (partnerRow != null && partnerRow.getIsActive() == 1 && partnerRow.isOmsAutoCloseOnReceipt() == true) {
      return true;
    }
    
    if (!order.isIsSpot()) {
      for (OrderLine line : order.getOrderLines()) {
        for (RequestSchedule rs : line.getRequestSchedules()) {

          // Last lookup option if flag is disabled at partner level: ACL/AVL 
          if(OrderTypeEnum.SALES_ORDER.toString().equals(order.getOrderType()) && !FieldUtil.isNull(line.getSysItemId())) {

            AclLine aclLine = OrderUtil.getACLFromRS(rs, ctx);
            if( aclLine != null ) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                  "ACL Line: [ Customer Partner =" + aclLine.getCustomerPartnerName() + " Item Name =" + aclLine.getItemName()
                  + " TCO Name =" + aclLine.getTCOName() + "]");
              }

              AclLineMDFs aclLineMDFs = aclLine.getMDFs(AclLineMDFs.class);
              if(aclLineMDFs.isAutoCloseOnReceipt()) {
                return true;
              }
            }

          } else {

            AvlLine avlLine = OrderUtil.getAVLFromRS(rs, ctx);
            if (null != avlLine) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                  "AVL Line: [ Vendor Partner =" + avlLine.getPartnerName() + " Item Name =" + avlLine.getItemName()
                  + " Site Name =" + avlLine.getSiteName() + "]");
              }

              AvlLineMDFs avlLineMDFs = avlLine.getMDFs(AvlLineMDFs.class);
              if (avlLineMDFs.isAutoCloseOnReceipt()) {
                return true;
              }
            }

          }
        }
        // END FOR
      }
    }
    return false;
  }

  /**
   * get Buyer acceptance mode as per BuyerAcceptance Enum
   *
   * @param order
   * @param ctx
   * @return BuyerAcceptanceEnum
   */
  public static BuyerAcceptanceEnum lookupBuyerAcceptanceMode(EnhancedOrder order, PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "lookupBuyerAcceptanceMode")) {
      BuyerAcceptanceEnum defaultValue = null;
      if (isDeploymentOrder(order)) {
        String doAutoFlag = getConsigneeOrderAcceptance(order, ctx);
        BuyerAcceptanceEnum result = BuyerAcceptanceEnum.get(doAutoFlag);
        if (result == null)
          result = defaultValue;
        return result;
      }
      if (!order.isIsSpot() && !isContract(order)) {
        OrderLine firstOrderLine = order.getOrderLines().get(0);
        RequestSchedule firstRs = firstOrderLine.getRequestSchedules().get(0);
        AvlLine avlLine = OrderUtil.getAVLFromRS(firstRs, ctx);
        if (null != avlLine) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
              "AVL Line: [ Partner =" + avlLine.getPartnerName() + " Item Name =" + avlLine.getItemName()
                + " Site Name =" + avlLine.getSiteName() + "]");
          }

          AvlLineMDFs avlLineMDFs = avlLine.getMDFs(AvlLineMDFs.class);
          if (!FieldUtil.isNull(avlLineMDFs.getBuyerAutoApproval())
            && !StringUtil.isNullOrBlank(avlLineMDFs.getBuyerAutoApproval())) {
            return BuyerAcceptanceEnum.get(avlLineMDFs.getBuyerAutoApproval());
          }
        }
      }

      PartnerRow partnerRow = PartnerUtil.getPartner(order.getSysVendorId(), ctx);
      if (partnerRow != null && partnerRow.getIsActive() == 1
        && !FieldUtil.isNull(partnerRow.getOmsBuyerAutoApproval())) {
        return BuyerAcceptanceEnum.get(partnerRow.getOmsBuyerAutoApproval());
      }
      return defaultValue;
    }

  }

  public static AutoPoAckModeEnum lookupAutoPoAckMode(EnhancedOrder order, PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getEnabledAutoFlag")) {
      AutoPoAckModeEnum defaultValue = null;
      if (isDeploymentOrder(order)) {
        String doAutoFlag = getShipperOrderAcceptance(order, ctx);
        AutoPoAckModeEnum result = AutoPoAckModeEnum.get(doAutoFlag);
        if (result == null)
          result = defaultValue;
        return result;
      }
      else if (!order.isIsSpot() && !isContract(order) && !isSalesOrder(order)) {
        AvlLine avlLine = null;
        for (OrderLine line : order.getOrderLines()) {
          for (RequestSchedule rs : line.getRequestSchedules()) {
            avlLine = OrderUtil.getAVLFromRS(rs, ctx);
            if (null != avlLine) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(
                  "AVL Line: [ Partner =" + avlLine.getPartnerName() + " Item Name =" + avlLine.getItemName()
                    + " Site Name =" + avlLine.getSiteName() + "]");
              }

              AvlLineMDFs avlLineMDFs = avlLine.getMDFs(AvlLineMDFs.class);
              if (!FieldUtil.isNull(avlLineMDFs.getAutoPoAckMode())) {
                return AutoPoAckModeEnum.get(avlLineMDFs.getAutoPoAckMode());
              }
            }
          }
        }
      } 
      if(!FieldUtil.isNull(order.getSysVendorId())){
    	  PartnerRow partnerRow = PartnerUtil.getPartner(order.getSysVendorId(), ctx);
    	  if (partnerRow != null && partnerRow.getIsActive() == 1 && !FieldUtil.isNull(partnerRow.getOmsAutoPoAckMode())) {
    		  return AutoPoAckModeEnum.get(partnerRow.getOmsAutoPoAckMode());
    	  }
      }
      return defaultValue;
    }
  }

  public static boolean isBuyerAutoApprovalEnabled(OrderLine line, PlatformUserContext ctx) {
    String buyerAutoApprovalMode = null;
    EnhancedOrder order = line.getParent();
    if (!order.isIsSpot()) {
      for (RequestSchedule rs : line.getRequestSchedules()) {
        AvlLine avlLine = getAVLFromRS(rs, ctx);
        if (avlLine != null) {
          buyerAutoApprovalMode = avlLine.getMDFs(AvlLineMDFs.class).getBuyerAutoApproval();
          if (!FieldUtil.isNull(buyerAutoApprovalMode)
            && (buyerAutoApprovalMode.equalsIgnoreCase(BuyerAcceptanceEnum.ACCEPT_TO_NEW.stringValue())
              || buyerAutoApprovalMode.equalsIgnoreCase(
                BuyerAcceptanceEnum.ACCEPT_TO_NEW_OPEN_ALLOW_CHANGE.stringValue())
              || buyerAutoApprovalMode.equalsIgnoreCase(
                BuyerAcceptanceEnum.ACCEPT_TO_NEW_OPEN_DISALLOW_CHANGE.stringValue()))) {
            return true;
          }
        }
      }
    }
    else {
      PartnerRow partnerRow = PartnerUtil.getPartner(order.getSysVendorId(), ctx);
      if (partnerRow != null && partnerRow.getIsActive() == 1) {
        buyerAutoApprovalMode = partnerRow.getOmsBuyerAutoApproval();
        if (!FieldUtil.isNull(buyerAutoApprovalMode)
          && (buyerAutoApprovalMode.equalsIgnoreCase(BuyerAcceptanceEnum.ACCEPT_TO_NEW.stringValue())
            || buyerAutoApprovalMode.equalsIgnoreCase(BuyerAcceptanceEnum.ACCEPT_TO_NEW_OPEN_ALLOW_CHANGE.stringValue())
            || buyerAutoApprovalMode.equalsIgnoreCase(
              BuyerAcceptanceEnum.ACCEPT_TO_NEW_OPEN_DISALLOW_CHANGE.stringValue()))) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Determine order international status and update order accordingly.
   * Set international status if it is NOT set.
   *
   * @param order
   * @param ctx TODO
   */
  public static void computeOrderInternationalStatus(EnhancedOrder order, PlatformUserContext ctx) {
	  if (OriginEnum.UI.stringValue().equals(order.getOrigin())) {
		  getShipFromCountry(order.getFirstDeliverySchedule(), ctx).ifPresent(shipFrom -> {
			  getShipToCountry(order.getFirstRequestSchedule(), ctx).ifPresent(shipTo -> {
				  order.setInternational(!shipFrom.equals(shipTo));
			  });
		  }); 
	  } 
  }

  private static Optional<String> getShipToCountry(RequestSchedule rs, PlatformUserContext ctx) {
    Optional<RequestSchedule> optRs = Optional.ofNullable(rs);
    Optional<String> fromSchedule = optRs.map(RequestSchedule::getShipToAddress).map(
      address -> address.get(AddressComponentType.COUNTRY));
    if (fromSchedule.isPresent())
      return fromSchedule;
    return optRs.map(RequestSchedule::getSysShipToSiteId).map(id -> OrderUtil.getSiteById(id, ctx)).map(
      SiteRow::getAddrCountry);
  }

  private static Optional<String> getShipFromCountry(DeliverySchedule ds, PlatformUserContext ctx) {
    Optional<DeliverySchedule> optDs = Optional.ofNullable(ds);
    Optional<String> fromSchedule = optDs.map(DeliverySchedule::getShipFromAddress).map(
      address -> address.get(AddressComponentType.COUNTRY));
    if (fromSchedule.isPresent())
      return fromSchedule;
    return optDs.map(DeliverySchedule::getSysShipFromSiteId).map(id -> OrderUtil.getSiteById(id, ctx)).map(
      SiteRow::getAddrCountry);
  }

  private static SiteRow getSiteById(long id, PlatformUserContext ctx) {
    return SiteCacheManager.getInstance().getRow(id, (DvceContext) ctx);
  }

  private static EquipmentType getEquipmentType(EnhancedOrder order, DvceContext dvceCtx) throws VCBaseException {
    TransMode transMode = TransModeUtil.getDefaultTransMode(order);
    long transModeId = -1L;
    if(null != transMode && !FieldUtil.isNull(transMode.id)) {
      transModeId = transMode.id;
    }
    EquipmentTypeCacheManager cacheManager = EquipmentTypeCacheManager.getInstance();
    EquipmentTypeRow row = cacheManager.getEquipmentType(transModeId, dvceCtx);
    if (row != null) {
      return (EquipmentType) EquipmentTypeHelper.getInstance().convertRowToJaxb(row, dvceCtx);
    }
    return null;
  }
  
  public static void updateOrderTotals(EnhancedOrder order, PlatformUserContext ctx) {
	  updateOrderTotals(order, null, ctx);
  }
  

  
  
  public static List<EnhancedOrder> processRPLOrderForSplitPercSourcing(List<EnhancedOrder> rplOrders, PlatformUserContext ctx){
    List<EnhancedOrder> orders = new ArrayList<EnhancedOrder>();
    LocalizationHelper helper = new LocalizationHelper(LocaleManager.getLocale());
    if(!rplOrders.isEmpty()) {
      for(EnhancedOrder rplOrder: rplOrders) {
        if (FieldUtil.isNull(rplOrder.getOrderLines().get(0).getRequestSchedules().get(0).getSysShipToSiteId())) {
          continue;
        }
        List<SourcingResponse> sourcingResponses= new ArrayList<SourcingResponse>();
        try {
        line:  for (OrderLine orderLine : rplOrder.getOrderLines()) {
            double orderQty = 0.0;
            for (DeliverySchedule ds :   getAllDeliverySchedules(orderLine)) {
              orderQty = orderQty + ds.getRequestQuantity();
            }
            for (RequestSchedule reqSchedule : orderLine.getRequestSchedules()) {
              String policyName = getAvlSourcingPolicy(reqSchedule.getSysShipToSiteId(), orderLine.getSysItemId(), reqSchedule.getSysShipToLocationId());
              if(FieldUtil.isNull(policyName) || (!FieldUtil.isNull(policyName) &&
                !policyName.equalsIgnoreCase(SourcingPolicyNameEnum.SPLITBETWEENVENDORSBYSPLITPERCENTAGEAVL.stringValue()))) {
                continue;
              }
              Date requestDate = null;
              Calendar requestDateCal = null;
              try {
                requestDateCal = OMSUtil.getDateByCollaboration(rplOrder, reqSchedule, (DvceContext) ctx);
              }
              catch (Exception e) {
                LOG.error("AVL Exception : Error while calculating request Date calendar");
              }
              if (FieldUtil.isNull(orderLine.getQuantityUOM())) {
                orderLine.setQuantityUOM(
                  OrderUtil.getQuantityUOM(
                    orderLine.getSysItemId(),
                    reqSchedule.getSysShipToSiteId(),
                    reqSchedule.getSysShipToLocationId(),
                    ctx));
              }

              if (null == requestDateCal) {
                LOG.error("The Requested Date by Partner Collaboration Type is NULL.");
              }
              else {
                requestDate = requestDateCal.getTime();
              }

              SourcingRequest request = new SourcingRequest(
                rplOrder.getValueChainId(),
                rplOrder.getOrderLines().get(0).getRequestSchedules().get(0).getSysShipToSiteId(),
                rplOrder.getOrderLines().get(0).getRequestSchedules().get(0).getSysShipToLocationId(),
                rplOrder.getOrderLines().get(0).getSysItemId(),
                rplOrder.getSysBuyingOrgId(),
                rplOrder.getOrderLines().get(0).getSysProductGroupLevelId(),
                rplOrder.getSysVendorId(),
                orderQty,
                QuantityUOM.fromString(rplOrder.getOrderLines().get(0).getQuantityUOM()).toInteger(),
                requestDate,
                null);
             sourcingResponses.addAll(AvlLineSourcingService.getSourcingResponse(ctx, request, policyName));
             break line;
            }
          }
        } catch (Exception e) {
          LOG.info("AVL Exception - "+e.getMessage());
          e.printStackTrace();
          String msg = helper.getLabel("OMS.enhancedOrder.ExceptionOnAvlLineSourcing",rplOrder.getVendorName(),rplOrder.getOrderLines().get(0).getItemName());
          rplOrder.setError(msg);
        }
        Boolean errorExits = false;
        String errorMessage = null;
        for(SourcingResponse res:sourcingResponses) {
          if(!res.isSuccess()) {
            errorExits = true;
            errorMessage = res.getErrorMessage();
            break;
          }
        }
        if(errorExits) {
          rplOrder.setError(errorMessage);
          orders.add(rplOrder);
          continue;
        }
        
       
        int i=0;
        for(SourcingResponse res:sourcingResponses) {
          if(i == 0) {
            i++;
            setSourcingResponseToOrder(rplOrder,res);
            orders.add(rplOrder);
          } else {
            JSONObject json = rplOrder.toJSONObject();
            EnhancedOrder clonedOrder = Model.fromJSONObject(json, EnhancedOrder.class);
            clonedOrder.setOrderNumber(null);
            clonedOrder.setAuxiliaryKey(null);
            setSourcingResponseToOrder(clonedOrder,res);
            orders.add(clonedOrder);
          }          
        }
          
      }
    }
    return orders;
    
  }
  
  private static void setSourcingResponseToOrder(EnhancedOrder order, SourcingResponse res) {
    Boolean sameVendor = false;
    if(!FieldUtil.isNull(res.getVendorId())) {
      if(!FieldUtil.isNull(order.getSysVendorId()) && order.getSysVendorId().equals(res.getVendorId())) {
        sameVendor=true;
      }
      order.setSysVendorId(res.getVendorId(), true);
      if(!FieldUtil.isNull(res.getAvlLineRow().getVendorOrgId())) {
        order.setSysSellingOrgId(res.getAvlLineRow().getVendorOrgId(), true);
        order.setSellingOrgEnterpriseName(null);
        order.setSellingOrgName(null);
      }
    }
    for(DeliverySchedule ds:OrderUtil.getAllDeliverySchedules(order)) {
      ds.setRequestQuantity(res.getRequestQuantity());
      if(!sameVendor) {
        ds.setSysShipFromSiteId(res.getAvlLineRow().getSysDefaultShipFromSiteId(), true);
        ds.setShipFromSiteOrganizationName(null);
        ds.setShipFromSiteEnterpriseName(null);
        ds.setShipFromSiteName(null);
      } else if(sameVendor &&  FieldUtil.isNull(ds.getSysShipFromSiteId())) {
        ds.setSysShipFromSiteId(res.getAvlLineRow().getSysDefaultShipFromSiteId(), true);
      }
      
    }
  }
  
  
  public static String getAvlSourcingPolicy(Long siteId, Long itemId, Long locationId) {
    String policyName = null;
    BufferRow bufferRow = AvlLineSourcingDao.getBuffer(siteId, itemId, locationId);
    ItemRow itemRow = ItemCacheManager.getInstance().getItem(itemId);
    // if buffer not found look for item policy
    if (bufferRow != null) {
      policyName = bufferRow.getOmsSourcingPolicyName();
    } else {
      policyName = itemRow.getOmsSourcingPolicyName();
    }
    return policyName;
  }

  /**
   * @param orgPolicyMap 
 * @see com.ordermgmtsystem.oms.domain.base.EnhancedOrderDomain#updateOrderTotals(com.ordermgmtsystem.supplychaincore.model.EnhancedOrder)
   */
  public static void updateOrderTotals(EnhancedOrder order, Map<String, List<OmsOrgProcurmntPolicyRow>> orgPolicyMap, PlatformUserContext ctx,List<BaseOrderLineForTotals> baseLines) {

    String psrKey = null;
    try {
      if (PSRLogger.isEnabled())
        psrKey = PSRLogger.enter(PSR_ID + "updateOrderTotals");
      try {
        boolean isNMD = false;
        double totalAmt = 0D;
        boolean isReturnOrder = OrderTypeEnum.RETURN_ORDER.toString().equals(order.getOrderType()) ? true : false;
        List<String> cancelledStates = new ArrayList<String>();
        cancelledStates.add(States.CANCELLED);
        cancelledStates.add(States.DELETED);
        cancelledStates.add(States.VENDOR_REJECTED);
        cancelledStates.add(States.BACKORDERED);
       if(baseLines==null)
         baseLines = BaseOrderLinesUtil.convertOrderLineToBaseOrderLine(
          order.getValueChainId(),
          order,
          true,
          cancelledStates,
          false,
          ctx);
        EquipmentType equipmentType = getEquipmentType(order, (DvceContext) ctx);

        OrderLine orderLine = order.getOrderLines().get(0);
        if (!FieldUtil.isNull(orderLine.getExtItemName())) {
          isNMD = true;
        }
        for (OrderLine line : order.getOrderLines()) {
          if (isNMD && !cancelledStates.contains(line.getState())) {
            if (!FieldUtil.isNull(line.getLineAmount()))
              totalAmt += line.getLineAmount();
          }
          else if (cancelledStates.contains(line.getState())) {
            line.setLineAmount(DvceConstants.NULL_DOUBLE_VALUE);
          }
        }
        boolean calculateOrderTotals = true;
        if(orgPolicyMap == null) {
          calculateOrderTotals = OrgProcPolicyUtil.getBooleanValue(
                    (DvceContext) ctx,
                    ((DvceContext) ctx).getUserContext().getValueChainId(),
                    order.getSysBuyingOrgId(),
                    OrgProcPolicyConstants.CALCULATE_ORDER_TOTALS,
                    true);
        } else {
          calculateOrderTotals = OrgProcPolicyUtil.getBooleanPropertyValue(orgPolicyMap, OrgProcPolicyConstants.CALCULATE_ORDER_TOTALS, 
              order, order.getSysBuyingOrgId(), Boolean.TRUE.toString());
        }
            
        
       
        OrderTotals orderTotals = new OrderTotals(
          order.getValueChainId(),
          baseLines,
          equipmentType,
          order.getQuantityUom(),
          ctx);
        double additionalCostTotal = 0;
        if (!FieldUtil.isNull(EnhancedOrderMDFs.from(order).getSysAdditionalCostId())) {
          ModelRetrieval modelRetrieval = ModelQuery.retrieve(AdditionalCost.class,CostComponent.class);
          modelRetrieval.setIncludeAttachments(AdditionalCost.class, false);
          modelRetrieval.setIncludeAttachments(CostComponent.class, false);
          AdditionalCost cost = DirectModelAccess.readById(
            AdditionalCost.class,
            EnhancedOrderMDFs.from(order).getSysAdditionalCostId(),
            ctx, modelRetrieval);
          additionalCostTotal = AdditionalCostUtil.getConvertedTotal(cost);
        }
        if (isNMD) {
          if (calculateOrderTotals)
            order.setTotalAmount(totalAmt + additionalCostTotal);
        }
        else if (orderTotals != null) {
          if (calculateOrderTotals) {
            order.setTotalQuantity(
              FieldUtil.isNull(orderTotals.getTotalQuantity())
              ? DvceConstants.NULL_DOUBLE_VALUE
                : orderTotals.getTotalQuantity().doubleValue());
            order.setTotalAmount(orderTotals.getTotalCost() + additionalCostTotal);
          order.setTotalVolume(orderTotals.getTotalVolume().floatValue());
          order.setTotalWeight(orderTotals.getTotalWeight().floatValue());
          order.setQuantityUom(orderTotals.getTotalQuantityUom().toString());
          order.setVolumeUom(
            orderTotals.getTotalVolumeUom() == null
              ? DvceConstants.NULL_STRING_VALUE
              : orderTotals.getTotalVolumeUom().toString());
          order.setWeightUom(
            orderTotals.getTotalWeightUom() == null
              ? DvceConstants.NULL_STRING_VALUE
              : orderTotals.getTotalWeightUom().toString());
          order.setTotalScaleUpQuantity(
            orderTotals.getTotalScaleUpQuantity() == null
              ? DvceConstants.NULL_FLOAT_VALUE
              : orderTotals.getTotalScaleUpQuantity().floatValue());
          order.setTotalScaleUpVolume(orderTotals.getTotalScaleUpVolume());
          order.setTotalScaleUpWeight(orderTotals.getTotalScaleUpWeight());
        }
          else {
            for (OrderLine line : order.getOrderLines()) {
              if (!line.isSetUnitPrice()) {
                line.setUnitPrice(0.0d);
              }
            }
          }
        }
        if (FieldUtil.isNull(order.getCurrency()) && !isNMD) {
          line: for (OrderLine line : order.getOrderLines()) {
            for (RequestSchedule rs : line.getRequestSchedules()) {
              if (nonTransitionalStates.contains(rs.getState()))
                continue;
              /*
               * If currency not present on order, fetch from Buffer. If not on Buffer, fetch from item
               */
              if(!FieldUtil.isNull(line.getCurrency())) {
                order.setCurrency(line.getCurrency());
              } else {
              order.setCurrency(
                populateCurrency(rs.getSysShipToSiteId(), line.getSysItemId(), rs.getSysShipToLocationId()));
              }
              if (FieldUtil.isNull(order.getCurrency()) && !FieldUtil.isNull(line.getSysGenericItemId()))
                order.setCurrency(
                  populateCurrency(rs.getSysShipToSiteId(), line.getSysGenericItemId(), rs.getSysShipToLocationId()));
              if (!FieldUtil.isNull(order.getCurrency()))
                break line;
            }
          }
        }
        else if (FieldUtil.isNull(order.getCurrency())) {
          ItemRow itemRow = null;
          if (!FieldUtil.isNull(orderLine.getSysItemId()))
            itemRow = OMSUtil.getItem(orderLine.getSysItemId());
          if (itemRow != null && !FieldUtil.isNull(itemRow.getCurrency())) {
            PdfStringEnum currencyCodeTypeEnum = EnumerationFactory.getPdfStringEnum("CurrencyCode");
            String currencyName = currencyCodeTypeEnum.getEnumName(itemRow.getCurrency().intValue());
            order.setCurrency(currencyName);
          }
        }
        //--If still the currency on order is null, set it to USD
        if (FieldUtil.isNull(order.getCurrency())) {
          order.setCurrency(CurrencyCode.USD.toString());
        }
        if (calculateOrderTotals) {
          populateOrderTotals(order, cancelledStates, additionalCostTotal, ctx);
        }
        if (!isReturnOrder) {
            validateZeroPrice(order, ctx);
          }
        validateZeroPricePer(order, ctx);
      }
      catch (Exception e) {
        if (!order.isSetError()) {
          order.setError("meta.error.enhancedOrder");
        }
        LOG.error("EXCEPTION", e);
      }
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
  
  }
  
  /**
   * @param orgPolicyMap 
 * @see com.ordermgmtsystem.oms.domain.base.EnhancedOrderDomain#updateOrderTotals(com.ordermgmtsystem.supplychaincore.model.EnhancedOrder)
   */
  public static void updateOrderTotals(EnhancedOrder order, Map<String, List<OmsOrgProcurmntPolicyRow>> orgPolicyMap, PlatformUserContext ctx) {
    updateOrderTotals(order,  orgPolicyMap, ctx,null);
  }

  /**
   * Populate Order totals
   *
   * @param order
   * @param cancelledStates
   * @param additionalCostTotal
   */
  private static void populateOrderTotals(
    EnhancedOrder order,
    List<String> cancelledStates,
    double additionalCostTotal,
    PlatformUserContext ctx) {
    boolean updateTotal = true;
    boolean isPriceCollaborationEnabled = false;
    String collaborativeFields = OrderUtil.getCollaborativeFields(
      order,
      Services.get(UserContextService.class).createDefaultEnterpriseAdminContext(
        order.getValueChainId(),
        order.getOwningOrgEnterpriseName()));
    if (collaborativeFields != null && (collaborativeFields.contains(OmsConstants.COLLABORATIVE_FIELD_UNIT_PRICE)
      || collaborativeFields.contains(OmsConstants.COLLABORATIVE_FIELD_PRICE_PER))) {
      isPriceCollaborationEnabled = true;
    }
    boolean collabPerDS = TransactionCache.getOrgPolicy(
    	      OMSConstants.Policies.ENABLE_BUYER_COLLABORATION_PER_DS,
    	      order.getSysOwningOrgId(),
    	      false,
    	      Services.get(UserContextService.class).createDefaultEnterpriseAdminContext(
    	    	        order.getValueChainId(),
    	    	        order.getOwningOrgEnterpriseName()));    		
    double orderTotal = 0.0;
    Set<Double> unitPriceList = new HashSet<Double>(); // This will be used to clear line level unit price field if DS has different unit price 
    Set<Double> pricePerList = new HashSet<Double>(); // This will be used to clear line level price per field if DS has different price per
    for (OrderLine line : order.getOrderLines()) {
      OrderLineMDFs omsLineMdfs = line.getMDFs(OrderLineMDFs.class);
      double lineAmount = 0.0;
      unitPriceList.clear();
      pricePerList.clear();
      line.setCurrency(order.getCurrency());
      for (RequestSchedule rs : line.getRequestSchedules()) {
        for (DeliverySchedule ds : rs.getDeliverySchedules()) {
          DeliveryScheduleMDFs omsDsMdfs = ds.getMDFs(DeliveryScheduleMDFs.class);
          if ((!ds.isSetRequestUnitPriceAmount() || FieldUtil.isNull(ds.getRequestUnitPriceAmount()))
            && line.isSetUnitPrice() && line.getUnitPrice() != 0) {
            ds.setRequestUnitPriceAmount(line.getUnitPrice());
          }
          //ds should have price per mentioned

          if (!cancelledStates.contains(ds.getState())) {
            //If line is in Open state then consider agreed fields to calculate line total amount.
            if (SCCEnhancedOrderConstants.States.OPEN.equals(line.getState())
              && !FieldUtil.isNull(ds.getAgreedQuantity()) && ds.getAgreedQuantity() > 0) {
            	if (!FieldUtil.isNull(ds.getAgreedUnitPriceAmount()) 
                        && ds.getAgreedUnitPriceAmount() > 0.0 
                        && ds.getAgreedUnitPriceAmount() != line.getUnitPrice()) {
                    line.setUnitPrice(ds.getAgreedUnitPriceAmount());
                  }
              else if(!ds.isSetAgreedUnitPriceAmount() || FieldUtil.isNull(ds.getAgreedUnitPriceAmount())) {
                ds.setAgreedUnitPriceAmount(line.getUnitPrice());
              }
            	if (!FieldUtil.isNull(omsDsMdfs.getAgreedPricePer()) 
            	  && omsDsMdfs.getAgreedPricePer() > DvceConstants.EPSILON) {
                    if(omsDsMdfs.getAgreedPricePer() > 0 
                      && omsDsMdfs.getAgreedPricePer() != omsLineMdfs.getPricePer()) {
                      omsLineMdfs.setPricePer(omsDsMdfs.getAgreedPricePer());
                    }
                  }
              else if (!FieldUtil.isNull(omsLineMdfs.getPricePer()) && omsLineMdfs.getPricePer() > 0 ){
                omsDsMdfs.setAgreedPricePer(omsLineMdfs.getPricePer());
              }
              if (!FieldUtil.isNull(omsDsMdfs.getAgreedPricePer()) && omsDsMdfs.getAgreedPricePer() > 0) {
                lineAmount = lineAmount + ((ds.getAgreedQuantity() / omsDsMdfs.getAgreedPricePer())*ds.getAgreedUnitPriceAmount());
              }
              else {
                lineAmount = lineAmount + ds.getAgreedQuantity() * ds.getAgreedUnitPriceAmount();
              }
              unitPriceList.add(ds.getAgreedUnitPriceAmount());
              pricePerList.add(omsDsMdfs.getAgreedPricePer());
            }
            else {
              if (!FieldUtil.isNull(ds.getRequestUnitPriceAmount())
                    && ds.getRequestUnitPriceAmount() > 0.0 ) {
                line.setUnitPrice(ds.getRequestUnitPriceAmount());
              }
              else {
                ds.setRequestUnitPriceAmount(line.getUnitPrice());
              }
              if (!FieldUtil.isNull(omsDsMdfs.getRequestPricePer()) && omsDsMdfs.getRequestPricePer() > DvceConstants.EPSILON) {
                omsLineMdfs.setPricePer(omsDsMdfs.getRequestPricePer());
              }
              //ds should have price per mentioned
              if (!FieldUtil.isNull(omsDsMdfs.getRequestPricePer()) && omsDsMdfs.getRequestPricePer() > DvceConstants.EPSILON) {
                lineAmount = lineAmount + ((ds.getRequestQuantity() / omsDsMdfs.getRequestPricePer())*ds.getRequestUnitPriceAmount());
              }
              else if(!FieldUtil.isNull(ds.getRequestUnitPriceAmount())) {
                lineAmount = lineAmount + ds.getRequestQuantity() * ds.getRequestUnitPriceAmount();
              }
              unitPriceList.add(ds.getRequestUnitPriceAmount());
              pricePerList.add(omsDsMdfs.getRequestPricePer());
              line.setUnitPrice(ds.getRequestUnitPriceAmount());
              ds.setRequestUnitPriceUOM(line.getCurrency());
              if (!isPriceCollaborationEnabled && !collabPerDS) {
                break;
              }
            }
          }
        }
      }
      if (updateTotal) {
        line.setLineAmount(lineAmount);
        orderTotal = orderTotal + lineAmount;
      }
      if (unitPriceList.size() > 1) {
        line.setUnitPrice(DvceConstants.NULL_DOUBLE_VALUE);
      }
      if (pricePerList.size() > 1) {
        omsLineMdfs.setPricePer(DvceConstants.NULL_DOUBLE_VALUE);
      }
    }
    if (updateTotal) {
    	orderTotal = orderTotal > 0 ? orderTotal + additionalCostTotal : orderTotal;
    	order.setTotalAmount(orderTotal);
    }
  }

  private static void validateZeroPrice(EnhancedOrder order, PlatformUserContext ctx) {
    boolean isAllowZeroUnitPriceOnOrder = true;
    if (OrderTypeEnum.PURCHASE_ORDER.stringValue().equals(order.getOrderType())) {
        isAllowZeroUnitPriceOnOrder = OrgProcPolicyUtil.getBooleanValue(
          (DvceContext) ctx,
          ((DvceContext) ctx).getUserContext().getValueChainId(),
          order.getSysOwningOrgId(),
          OrgProcPolicyConstants.ALLOW_ZERO_PRICE_ON_PO,
          false);
    }
    if (!isAllowZeroUnitPriceOnOrder) {
      for (OrderLine line : order.getOrderLines()) {
        if (isCategoryLine(line) || nonTransitionalStates.contains(line.getState())) {
          continue;
        }
        for (RequestSchedule rs : line.getRequestSchedules()) {
          for (DeliverySchedule ds : rs.getDeliverySchedules()) {
            boolean setError = false;
            //If agreed,promise or request unit price is zero the show error 
            if ((ds.isSetAgreedUnitPriceAmount() && !FieldUtil.isNull(ds.getAgreedUnitPriceAmount())
              && ds.getAgreedUnitPriceAmount() == 0)
              || (ds.isSetPromiseUnitPriceAmount() && !FieldUtil.isNull(ds.getPromiseUnitPriceAmount())
                && ds.getPromiseUnitPriceAmount() == 0)
              || (ds.isSetRequestUnitPriceAmount() && !FieldUtil.isNull(ds.getRequestUnitPriceAmount())
                && ds.getRequestUnitPriceAmount() == 0)) {
              setError = true;
            }
            if (setError && !order.isSetError()) {
              line.setError("meta.error.enhancedOrder.ZeroUnitPriceError");
              if(!ExternalRefUtil.isPartialOrderRollbackEnabled(getCurrentActionName())) {
                order.setError("meta.error.enhancedOrder.ZeroUnitPriceError");
              }
            }
          }
        }
      }
    }
  }
  
  private static void validateZeroPricePer(EnhancedOrder order, PlatformUserContext ctx) {
    for (OrderLine line : order.getOrderLines()) {
      if (nonTransitionalStates.contains(line.getState())) {
        continue;
      }
      OrderLineMDFs omsLineMDFs = line.getMDFs(OrderLineMDFs.class);
      if (omsLineMDFs.isSetPricePer() && !FieldUtil.isNull(omsLineMDFs.getPricePer())
        && omsLineMDFs.getPricePer() <= 0) {
        line.setError("meta.error.enhancedOrder.ZeroPricePerError");
      }
      for (RequestSchedule rs : line.getRequestSchedules()) {
        for (DeliverySchedule ds : rs.getDeliverySchedules()) {
          DeliveryScheduleMDFs omsDsMDFs = ds.getMDFs(DeliveryScheduleMDFs.class);
          boolean setError = false;
          //If agreed,promise or request price per is zero or negative then show error 
          if ((omsDsMDFs.isSetAgreedPricePer() && !FieldUtil.isNull(omsDsMDFs.getAgreedPricePer())
            && omsDsMDFs.getAgreedPricePer() <= 0)
            || (omsDsMDFs.isSetPromisePricePer() && !FieldUtil.isNull(omsDsMDFs.getPromisePricePer())
              && omsDsMDFs.getPromisePricePer() <= 0)
            || (omsDsMDFs.isSetRequestPricePer() && !FieldUtil.isNull(omsDsMDFs.getRequestPricePer())
              && omsDsMDFs.getRequestPricePer() <= 0)) {
            setError = true;
          }
          if (setError && !order.isSetError()) {
            line.setError("meta.error.enhancedOrder.ZeroPricePerError");
            order.setError("meta.error.enhancedOrder.ZeroPricePerError");
          }
        }
      }
    }
  }

  public static String populateCurrency(Long shipToSiteId, Long itemId, Long locationId) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "populateCurrency")) {
      BufferRow bufferRow = null;
      if (shipToSiteId == null)
        return null;

      try {
        if (FieldUtil.isNull(locationId)) {
          bufferRow = OMSUtil.getBuffer(shipToSiteId, itemId);
        }
        else {
          bufferRow = OMSUtil.getBuffer(shipToSiteId, itemId, locationId);
        }

        PdfStringEnum currencyCodeTypeEnum = EnumerationFactory.getPdfStringEnum("CurrencyCode");
        if (bufferRow != null && !FieldUtil.isNull(bufferRow.getCurrency())) {
          if (!FieldUtil.isNull(bufferRow.getCurrency().intValue()) && currencyCodeTypeEnum != null)
            return currencyCodeTypeEnum.getEnumName(bufferRow.getCurrency().intValue());
        }
        else {
          ItemRow itemRow = OMSUtil.getItem(itemId);
          if (itemRow != null && !FieldUtil.isNull(itemRow.getCurrency())) {
            if (!FieldUtil.isNull(itemRow.getCurrency().intValue()) && currencyCodeTypeEnum != null)
              return currencyCodeTypeEnum.getEnumName(itemRow.getCurrency().intValue());
          }
        }
      }
      catch (Exception exception) {
        LOG.error("Site = " + shipToSiteId + ", Item = " + itemId + ", Location = " + locationId);
      }
      return null;
    }
  }

  public static String getTemplateName(Long sysOrderId) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getTemplateName")) {
      SqlService service = Services.get(SqlService.class);
      String sql = "select name from BASE_TEMPLATE"
        + "  where SYS_BASE_TEMPLATE_ID = (select SYS_BASE_TEMPLATE_ID from TEMPLATE where SYS_TEMPLATE_ID ="
        + "      (select SYS_CREATION_TEMPLATE_ID from scc_enhanced_order where SYS_ENHANCED_ORDER_ID=$SysEnhancedOrderId$))";
      SqlParams params = new SqlParams();
      params.setLongValue("SysEnhancedOrderId", sysOrderId);
      SqlResult result = service.executeQuery(sql, params);
      String templateName = result.getRows().size() == 0 ? null : result.getRows().get(0).getStringValue("NAME");
      return templateName;
    }
  }

  public static boolean isGenericItemBased(EnhancedOrder order) {
    for (OrderLine orderLine : order.getOrderLines()) {
      if ((!FieldUtil.isNull(orderLine.getSysGenericItemId()) || !FieldUtil.isNull(orderLine.getGenericItemName()))
        || (!FieldUtil.isNull(orderLine.getSysSpecificItemId())
          || !FieldUtil.isNull(orderLine.getSpecificItemName()))) {
        return true;
      }
    }
    return false;
  }

  public static void populateTemplateId(List<EnhancedOrder> orders) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "populateTemplateId")) {
    	Map<EnhancedOrder, List<TemplateKey>> templateMap= new HashMap<EnhancedOrder, List<TemplateKey>>();
    	List<TemplateKey> templateKeys = new ArrayList<TemplateKey>();
      for (EnhancedOrder order : orders) {
        if (FieldUtil.isNull(order.getSysCreationTemplateId())) {         
          Long owningOrgId =   order.getSysOwningOrgId();
          Long vcId =   order.getValueChainId();
          String owningUserName =  null;            
          if (!FieldUtil.isNull(order.getCreationUser()) && !FieldUtil.isNull(order.getOrigin())
            && (OrderOriginEnum.UI.toString().equalsIgnoreCase(order.getOrigin()))) {
        	  owningUserName =  order.getCreationUser();
          } else {
        	  owningUserName =  "";
          }
          String baseTemplateName = null;
          String levelType = null;
          String orderType = order.getOrderType();
          if (isGenericItemBased(order)) {
            if (OrderTypeEnum.SALES_ORDER.stringValue().equals(orderType)) {
              baseTemplateName =OmsConstants.GENERIC_ITEM_SALES_ORDER_TEMPLATE ;
          	  levelType = "SCC.EnhancedOrder:Sales Order";
            }else if (OrderTypeEnum.DEPLOYMENT_ORDER.stringValue().equals(orderType)) {
            	baseTemplateName =OmsConstants.GENERIC_ITEM_DEPLOYMENT_ORDER_TEMPALTE ;
            	levelType = "SCC.EnhancedOrder:Deployment Order";
            }
            else if (OrderTypeEnum.RETURN_ORDER.stringValue().equals(orderType)) {
            	baseTemplateName =OmsConstants.GENERIC_ITEM_RETURN_ORDER_TEMPALTE ;
            	levelType = "SCC.EnhancedOrder:Return Order";
            }
            else if (order.isIsSpot()) {
            	baseTemplateName =OmsConstants.GENERIC_ITEM_SPOT_ORDER_TEMPALTE ;
            	levelType = "SCC.EnhancedOrder:Spot Order";
            }
            else if (isContract(order)) {
              if (order.isIsVMI()) {
            	baseTemplateName =OmsConstants.GENERIC_ITEM_VMI_PORELEASE_ORDER_TEMPALTE ;
              	levelType = "SCC.EnhancedOrder:VMIPORelease Order";
              }
              else {
            	baseTemplateName =OmsConstants.GENERIC_ITEM_PO_RELEASE_ORDER_TEMPLATE ;
              	levelType = "SCC.EnhancedOrder:PORelease Order";
              }
            }
            else {
            	baseTemplateName =OmsConstants.GENERIC_ITEM_ORDER_TEMPALTE ;
            	levelType = "SCC.EnhancedOrder:Purchase Order";
            }
          }
          else {
            if (OrderTypeEnum.DEPLOYMENT_ORDER.stringValue().equals(orderType)) {
            	baseTemplateName =OmsConstants.DEPLOYMENT_ORDER_BASE_TEMPALTE ;
            	levelType = "SCC.EnhancedOrder:Deployment Order";
            }
            else if (OrderTypeEnum.RETURN_ORDER.stringValue().equals(orderType)) {
            	baseTemplateName =OmsConstants.RETURN_ORDER_BASE_TEMPALTE ;
            	levelType = "SCC.EnhancedOrder:Return Order";
            }
            else if (OrderTypeEnum.SALES_ORDER.stringValue().equals(orderType)) {
            	baseTemplateName =OmsConstants.SALES_ORDER_BASE_TEMPALTE ;
            	levelType = "SCC.EnhancedOrder:Sales Order";
            }
            else if (order.isIsSpot()) {
            	baseTemplateName =OmsConstants.SPOT_ORDER_BASE_TEMPALTE ;
            	levelType = "SCC.EnhancedOrder:Spot Order";
            }
            else if (isContract(order)) {
              if (order.isIsVMI()) {
                baseTemplateName =OmsConstants.VMI_PORELEASE_BASE_TEMPALTE ;
                levelType = "SCC.EnhancedOrder:VMIPORelease Order";
              }else if(OrderUtil.isBlanketRelease(order)) {
                if(OrderUtil.isDeploymentOrder(order)) {
                  levelType = "SCC.EnhancedOrder:Blanket Release Deployment Order";
                  baseTemplateName = OmsConstants.BO_DORELEASE_BASE_TEMPALTE;
                } else {
                  levelType = "SCC.EnhancedOrder:Blanket Release Purchase Order";
                  baseTemplateName = OmsConstants.BO_PORELEASE_BASE_TEMPALTE;
                }
              }else {
            	baseTemplateName =OmsConstants.PORELEASE_BASE_TEMPALTE ;
              	levelType = "SCC.EnhancedOrder:PORelease Order";
              }
            }
            else if (order.isIsVMI()) {
            	baseTemplateName =OmsConstants.VMI_ORDER_BASE_TEMPALTE ;
            	levelType = "SCC.EnhancedOrder:VMI Order";
            }
            else {
            	baseTemplateName =OmsConstants.ENHANCED_ORDER_ORDER_TEMPALTE+","+"StandardPOSchedulesBaseTemplate" ;
            	levelType = "SCC.EnhancedOrder:Purchase Order";
            }
          }
          TemplateKey baseTemplateKey = new TemplateKey(owningOrgId, vcId, owningUserName, baseTemplateName, levelType);
          List<TemplateKey> keys = new ArrayList<TemplateKey>();
          if(baseTemplateName.contains(",")) {
        	 String[] templates = baseTemplateName.split(",");
        	 for(int i =0;i<templates.length;i++) {
        		 TemplateKey templateKey = new TemplateKey(owningOrgId, vcId, owningUserName, templates[i], levelType);
        		 keys.add(templateKey);
        	 }
          } else {
        	 
     		 keys.add(baseTemplateKey);
          }
          
          if(!templateKeys.contains(baseTemplateKey))
  		     templateKeys.add(baseTemplateKey);
             templateMap.put(order, keys);   
        }
      }
      Map<TemplateKey,Long> templateIds = TransactionCache.getTemplateIds(templateKeys);
      Map<Long,Boolean> templateIdDefaultMap = new HashMap<Long,Boolean>();
      
      if(templateIds!= null && !templateMap.isEmpty() && !templateIds.isEmpty() ) {
    	  for(Entry<TemplateKey, Long> orderTemplate: templateIds.entrySet()) {
    		  TemplateKey key = orderTemplate.getKey(); 
    		  Long tempId = orderTemplate.getValue();
    		  for(Entry<EnhancedOrder, List<TemplateKey>> temp: templateMap.entrySet()) {
    			  List<TemplateKey> tempKeys = temp.getValue(); 
        		  EnhancedOrder order = temp.getKey();
        		  if((FieldUtil.isNull(order.getSysCreationTemplateId()) || (key.isDefault && !FieldUtil.isNull(order.getSysCreationTemplateId()) 
        				  && templateIdDefaultMap.containsKey(order.getSysCreationTemplateId()) && !templateIdDefaultMap.get(order.getSysCreationTemplateId())) ) 
        				  && tempKeys!=null && tempKeys.contains(key) && !FieldUtil.isNull(tempId)) {
        			  order.setSysCreationTemplateId(tempId,true);
        			  templateIdDefaultMap.put(tempId, key.isDefault);
        		  }  
        		  
    		  }
    		  
    	  }
      }
      if(templateIds!= null && !templateMap.isEmpty() && !templateIds.isEmpty() ) {
    	  for(Entry<TemplateKey, Long> orderTemplate: templateIds.entrySet()) {
    		  TemplateKey key = orderTemplate.getKey(); 
    		  Long tempId = orderTemplate.getValue();
    		  for(Entry<EnhancedOrder, List<TemplateKey>> temp: templateMap.entrySet()) {
    			  List<TemplateKey> tempKeys = temp.getValue(); 
    			  List<TemplateKey> defaultKeys = new ArrayList<TemplateKey>();
    			  for(TemplateKey temKey:tempKeys) {
    				  TemplateKey templateKey = new TemplateKey(null, temKey.vcId, temKey.getOwningUserName(), temKey.baseTemplateName, temKey.levelType);
    				  defaultKeys.add(templateKey);
    			  }
        		  EnhancedOrder order = temp.getKey();
        		  if((FieldUtil.isNull(order.getSysCreationTemplateId()) || (key.isDefault && !FieldUtil.isNull(order.getSysCreationTemplateId()) 
        				  && templateIdDefaultMap.containsKey(order.getSysCreationTemplateId()) && !templateIdDefaultMap.get(order.getSysCreationTemplateId())) ) 
        				  && tempKeys!=null && defaultKeys.contains(key) && !FieldUtil.isNull(tempId)) {
        			  order.setSysCreationTemplateId(tempId,true);
        			  templateIdDefaultMap.put(tempId, key.isDefault);
        		  }  
        		  
    		  }
    		  
    	  }
      }
    }
  }

  public static boolean isOrderEditable(EnhancedOrder order, PlatformUserContext context) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "isOrderEditable")) {
      OrganizationRow oRow = OrganizationCacheManager.getInstance().getOrganization(order.getSysOwningOrgId());

      String policyName = "";
      if (!FieldUtil.isNull(order.getOrderType())) {
        if (OrderTypeEnum.PURCHASE_ORDER.toString().equals(order.getOrderType())) {
          policyName = OrgProcPolicyConstants.PURCHASE_ORDER_EDIT_MASK;
        }
        else if (OrderTypeEnum.DEPLOYMENT_ORDER.toString().equals(order.getOrderType())) {
          policyName = OrgProcPolicyConstants.DEPLOYMENT_ORDER_EDIT_MASK;
        }
        else if (OrderTypeEnum.SALES_ORDER.toString().equals(order.getOrderType())) {
          policyName = OrgProcPolicyConstants.SALES_ORDER_EDIT_MASK;
        }
        else if (OrderTypeEnum.RETURN_ORDER.toString().equals(order.getOrderType())) {
          policyName = OrgProcPolicyConstants.RETURN_ORDER_EDIT_MASK;
        }
      }
      if (oRow != null) {
        boolean isOrderNotEditableByType = OrgProcPolicyUtil.getBooleanValue(
          (DvceContext) context,
          order.getValueChainId(),
          oRow.getSysOrgId(),
          policyName,
          false);

        if (isOrderNotEditableByType && !DISABLE_ORDER_EDIT_BEYOND_OPEN_STATES.contains(order.getState())) {
          return false;
        }
      }
      return true;
    }
  }

  public static boolean isFromOrModifiedByEdi(EnhancedOrder order) {
    EnhancedOrderMDFs mdfs = order.getMDFs(EnhancedOrderMDFs.class);
    final String EDI = OriginEnum.EDI.stringValue();
    return EDI.equals(order.getOrigin()) || EDI.equals(mdfs.getSourceOfChange());
  }

  public static Class loadClass(Long vcId, String className, DvceContext dvceContext) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "loadClass")) {
      // Consider using EptClassLoader to load the given class
      SptClassLoader sptClassLoader = SptClassLoaderFactory.getSptClassLoader(vcId, dvceContext);
      try {
        Class sptClass = DynamicLoad.loadClass(sptClassLoader, className);
        return sptClass;
      }
      catch (Exception e) {
        LOG.error("--------------------> Unable to load validator class : " + className + " ", e);
        return null;
      }
    }
  }

  /**
   * Method to load and fetch DuplicateItemValidator type class
   *
   * @param order
   * @param className
   * @param dvceContext
   * @return
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public static DuplicateItemsValidator getDuplicateItemValidator(
    EnhancedOrder order,
    String className,
    DvceContext dvceContext)
    throws InstantiationException, IllegalAccessException {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getDuplicateItemValidator")) {
      DuplicateItemsValidator validator = null;
      Class validatorClass = loadClass(order.getValueChainId(), className, dvceContext);
      if (validatorClass != null) {
        try {
          validator = DuplicateItemsValidator.class.cast(validatorClass.newInstance());
          return validator;
        }
        catch (ClassCastException ce) {
          //-- This implies the custom validator class in policyValue is not of type DuplicateItemsValidator
          LOG.error(
            "---> Given class " + className + " is NOT of type DuplicateItemsValidator. "
              + "Hence, default class will be used to validate duplicate items.");
          validatorClass = loadClass(order.getValueChainId(), "DuplicateItemsValidatorImpl", dvceContext);
          return DuplicateItemsValidator.class.cast(validatorClass.newInstance());
        }
      }
      else {
        LOG.error("------> Missing class for DuplicateItemValidation - " + className);
        //-- Validate using default validator for duplicate item check
        validatorClass = loadClass(order.getValueChainId(), "DuplicateItemsValidatorImpl", dvceContext);
        return DuplicateItemsValidator.class.cast(validatorClass.newInstance());
      }
    }
  }

  public static EDICodePopulator getEDICodePopulator(EnhancedOrder order, DvceContext dvceContext)
    throws InstantiationException, IllegalAccessException {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getEDICodePopulator")) {
      EDICodePopulator populator = null;
      String className = null;
      String policyName = OrgProcPolicyConstants.EDI_CODE_POPULATOR_PREFIX + order.getOrderType();
      OrganizationRow org = null;

      if (!FieldUtil.isNull(order.getSysOwningOrgId())) {
        org = OrganizationCacheManager.getInstance().getOrganization(order.getSysOwningOrgId());
      }
      else if (!FieldUtil.isNull(order.getOwningOrgName()) && !FieldUtil.isNull(order.getOwningOrgEnterpriseName())) {
        org = OrganizationCacheManager.getInstance().getOrganization(
          new OrganizationKey(order.getValueChainId(), order.getOwningOrgEnterpriseName(), order.getOwningOrgName()));
      }

      if (org == null)
        return null;

      className = OrgProcPolicyUtil.getStringValue(
        dvceContext,
        dvceContext.getValueChainId(),
        org.getSysOrgId(),
        policyName,
        "com.ordermgmtsystem.oms.customization.impl.EDICodePopulatorImpl");

      if (FieldUtil.isNull(className))
        return null;

      Class populatorClass = loadClass(order.getValueChainId(), className, dvceContext);
      if (populatorClass != null) {
        try {
          populator = EDICodePopulator.class.cast(populatorClass.newInstance());
          return populator;
        }
        catch (ClassCastException ce) {
          //-- This implies the custom populator class in policyValue is not of type EDICodePopulator
          LOG.error(
            "---> Given class " + className + " is NOT of type EDICodePopulator. "
              + "Hence, default class will be used to populate EDI Change Type Codes.");
          populatorClass = loadClass(
            order.getValueChainId(),
            "com.ordermgmtsystem.oms.customization.impl.EDICodePopulatorImpl",
            dvceContext);
          return EDICodePopulator.class.cast(populatorClass.newInstance());
        }
      }
      else {
        LOG.error("------> Missing class for EDICodePopulator - " + className);
        //-- Populate EDI Change Type Code using default populator
        populatorClass = loadClass(order.getValueChainId(), "EDICodePopulatorImpl", dvceContext);
        return EDICodePopulator.class.cast(populatorClass.newInstance());
      }
    }
  }

  /**
   * Utility method to generate Org. Proc. Policy Name for given policyNamePrefix and OrderType.
   * This method will only work for Policy Names which are of type PolicyName<.OrderType>
   *
   * @param policyNamePrefix
   * @param orderType
   * @return
   */
  public static String getPolicyNameForOrderType(String policyNamePrefix, String orderType) {
    if (FieldUtil.isNull(policyNamePrefix) || FieldUtil.isNull(orderType)) {
      LOG.error(
        "PolicyNamePrefix and OrderType are required to generate the policy name. Values provided policyNamePrefix :"
          + policyNamePrefix + " orderType: " + orderType);
      return null;
    }
    return policyNamePrefix + "." + orderType.replaceAll("\\s", "");
  }

  /**
   * Determines if an order should be treated as a generic/specific order.
   * @param platformUserContext 
   */
  public static boolean isOrderGeneric(EnhancedOrder order, PlatformUserContext platformUserContext) {
    return order.getOrderLines().stream().anyMatch(ol->!FieldUtil.isNull(ol.getSysGenericItemId()));
    }

  public static void clearEDI860ChangeTypeCode(EnhancedOrder order) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "clearEDI860ChangeTypeCode")) {
    for (OrderLine line : order.getOrderLines()) {
      for (RequestSchedule rs : line.getRequestSchedules()) {
        for (DeliverySchedule ds : rs.getDeliverySchedules()) {
          clearEDI860ChangeTypeCode(ds);
        }
      }
    }
    }
  }

  public static void clearEDI860ChangeTypeCode(DeliverySchedule ds) {
    DeliveryScheduleMDFs dsMdf = ds.getMDFs(DeliveryScheduleMDFs.class);
    dsMdf.setEdi860Code(EDIChangeTypeCodeEnum.NC.stringValue());
  }

  public static void clearEDI865ChangeTypeCode(DeliverySchedule ds) {
    DeliveryScheduleMDFs dsMdf = ds.getMDFs(DeliveryScheduleMDFs.class);
    dsMdf.setEdi865Code(EDIChangeTypeCodeEnum.NC.stringValue());
  }

  public static void setEDI860ChangeTypeCode(DeliverySchedule ds, String code) {
    DeliveryScheduleMDFs dsMdf = ds.getMDFs(DeliveryScheduleMDFs.class);
    EDIChangeTypeCodeEnum codeEnum = EDIChangeTypeCodeEnum.get(code);
    if (null == codeEnum) {
      LOG.info("Unable to setEDI860ChangeTypeCode for code: " + code);
    }
    else {
      dsMdf.setEdi860Code(codeEnum.stringValue());
    }
  }

  public static void updateEdi860ChangeTypeCode(
    EnhancedOrder inputOrder,
    EnhancedOrder currentOrder,
    PlatformUserContext ctx) {
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled())
        psrKey = PSRLogger.enter(PSR_ID + "updateEdi860ChangeTypeCode");

      EDICodePopulator populator = null;
      populator = OrderUtil.getEDICodePopulator(currentOrder, (DvceContext) ctx);
      if (populator != null) {
        populator.updateEdi860ChangeTypeCode(inputOrder, currentOrder, (DvceContext) ctx);
      }
    }
    catch (Exception e) {
      LOG.error("Error in updateEdi860ChangeTypeCode", e);
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
  }

  public static void updateEdi865ChangeTypeCode(
    EnhancedOrder inputOrder,
    EnhancedOrder currentOrder,
    PlatformUserContext ctx) {
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled())
        psrKey = PSRLogger.enter(PSR_ID + "updateEdi860ChangeTypeCode");

      EDICodePopulator populator = null;
      populator = OrderUtil.getEDICodePopulator(currentOrder, (DvceContext) ctx);
      if (populator != null) {
        populator.updateEdi865ChangeTypeCode(inputOrder, currentOrder, (DvceContext) ctx);
      }
    }
    catch (Exception e) {
      LOG.error("Error in updateEdi865ChangeTypeCode", e);
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
  }

  public static String getEDI860CodeForLine(OrderLine line, DvceContext ctx) {
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled())
        psrKey = PSRLogger.enter(PSR_ID + "getEDI860CodeForLine");

      EDICodePopulator populator = null;
      populator = OrderUtil.getEDICodePopulator(line.getParent(), (DvceContext) ctx);
      if (populator != null) {
        return populator.getEDI860CodeForLine(line, (DvceContext) ctx);
      }
    }
    catch (Exception e) {
      LOG.error("Error in getEDI860CodeForLine", e);
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
    return null;
  }

  public static String getEDI865CodeForLine(OrderLine line, DvceContext ctx) {
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled())
        psrKey = PSRLogger.enter(PSR_ID + "getEDI865CodeForLine");

      EDICodePopulator populator = null;
      populator = OrderUtil.getEDICodePopulator(line.getParent(), (DvceContext) ctx);
      if (populator != null) {
        return populator.getEDI865CodeForLine(line, (DvceContext) ctx);
      }
    }
    catch (Exception e) {
      LOG.error("Error in getEDI865CodeForLine", e);
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
    return null;
  }

  /**
   * Prepare Delivery schedule Map of Order. 
   * This can be used in cases where you want to compare input order details and current order details
   *
   * @param inputOrder
   */
  static Map<String, DeliverySchedule> prepareDeliveryScheduleMap(EnhancedOrder order) {
    Map<String, DeliverySchedule> dsMap = new HashMap<String, DeliverySchedule>();
    for (OrderLine line : order.getOrderLines()) {
      for (RequestSchedule rs : line.getRequestSchedules()) {
        for (DeliverySchedule ds : rs.getDeliverySchedules()) {
          dsMap.put(
            order.getOrderNumber() + line.getLineNumber() + rs.getRequestScheduleNumber()
              + ds.getDeliveryScheduleNumber(),
            ds);
        }
      }
    }
    return dsMap;
  }

  public static boolean isServiceLevelChanged(Model input, Model current) {
    boolean isDiff = false;
    String origin = "";
    Long inputSysServiceLevelId = null;
    Long currentSysServiceLevelId = null;

    String inputServiceLevelName = null;
    String currentServiceLevelName = null;

    String inputServiceLevelEnt = null;
    String currentServiceLevelEnt = null;

    String inputServiceLevelOrg = null;
    String currentServiceLevelOrg = null;

    if (input != null && current != null) {
      if (input instanceof EnhancedOrder) {
        EnhancedOrder inputOrder = (EnhancedOrder) input;
        EnhancedOrder currentOrder = (EnhancedOrder) current;
        origin = inputOrder.getOrigin();
        if (!FieldUtil.isNull(inputOrder.getSysServiceLevelId())) {
          inputSysServiceLevelId = inputOrder.getSysServiceLevelId();
        }
        if (!FieldUtil.isNull(currentOrder.getSysServiceLevelId())) {
          currentSysServiceLevelId = inputOrder.getSysServiceLevelId();
        }

        if (!FieldUtil.isNull(inputOrder.getServiceLevelName())) {
          inputServiceLevelName = inputOrder.getServiceLevelName();
        }
        if (!FieldUtil.isNull(currentOrder.getServiceLevelName())) {
          currentServiceLevelName = currentOrder.getServiceLevelName();
        }

        if (!FieldUtil.isNull(inputOrder.getServiceLevelOrgEnterpriseName())) {
          inputServiceLevelEnt = inputOrder.getServiceLevelOrgEnterpriseName();
        }
        if (!FieldUtil.isNull(currentOrder.getServiceLevelOrgEnterpriseName())) {
          currentServiceLevelEnt = currentOrder.getServiceLevelOrgEnterpriseName();
        }

        if (!FieldUtil.isNull(inputOrder.getServiceLevelOrgName())) {
          inputServiceLevelOrg = inputOrder.getServiceLevelOrgName();
        }
        if (!FieldUtil.isNull(currentOrder.getServiceLevelOrgName())) {
          currentServiceLevelOrg = currentOrder.getServiceLevelOrgName();
        }
      }
      else if (input instanceof OrderLine) {
        OrderLine inputLine = (OrderLine) input;
        OrderLine currentLine = (OrderLine) current;
        origin = inputLine.getParent().getOrigin();
        if (!FieldUtil.isNull(inputLine.getSysLnServiceLevelId())) {
          inputSysServiceLevelId = inputLine.getSysLnServiceLevelId();
        }
        if (!FieldUtil.isNull(currentLine.getSysLnServiceLevelId())) {
          currentSysServiceLevelId = currentLine.getSysLnServiceLevelId();
        }

        if (!FieldUtil.isNull(inputLine.getLnServiceLevelServiceLevelName())) {
          inputServiceLevelName = inputLine.getLnServiceLevelServiceLevelName();
        }
        if (!FieldUtil.isNull(currentLine.getLnServiceLevelServiceLevelName())) {
          currentServiceLevelName = currentLine.getLnServiceLevelServiceLevelName();
        }

        if (!FieldUtil.isNull(inputLine.getLnServiceLevelOrgEnterpriseName())) {
          inputServiceLevelEnt = inputLine.getLnServiceLevelOrgEnterpriseName();
        }
        if (!FieldUtil.isNull(currentLine.getLnServiceLevelOrgEnterpriseName())) {
          currentServiceLevelEnt = currentLine.getLnServiceLevelOrgEnterpriseName();
        }

        if (!FieldUtil.isNull(inputLine.getLnServiceLevelOrgName())) {
          inputServiceLevelOrg = inputLine.getLnServiceLevelOrgName();
        }
        if (!FieldUtil.isNull(currentLine.getLnServiceLevelOrgName())) {
          currentServiceLevelOrg = currentLine.getLnServiceLevelOrgName();
        }
      }
      else if (input instanceof RequestSchedule) {
        RequestSchedule inputRS = (RequestSchedule) input;
        RequestSchedule currentRS = (RequestSchedule) current;
        origin = inputRS.getParent().getParent().getOrigin();
        if (!FieldUtil.isNull(inputRS.getSysRsServiceLevelId())) {
          inputSysServiceLevelId = inputRS.getSysRsServiceLevelId();
        }
        if (!FieldUtil.isNull(currentRS.getSysRsServiceLevelId())) {
          currentSysServiceLevelId = currentRS.getSysRsServiceLevelId();
        }

        if (!FieldUtil.isNull(inputRS.getRsServiceLevelServiceLevelName())) {
          inputServiceLevelName = inputRS.getRsServiceLevelServiceLevelName();
        }
        if (!FieldUtil.isNull(currentRS.getRsServiceLevelServiceLevelName())) {
          currentServiceLevelName = currentRS.getRsServiceLevelServiceLevelName();
        }

        if (!FieldUtil.isNull(inputRS.getRsServiceLevelOrgEnterpriseName())) {
          inputServiceLevelEnt = inputRS.getRsServiceLevelOrgEnterpriseName();
        }
        if (!FieldUtil.isNull(currentRS.getRsServiceLevelOrgEnterpriseName())) {
          currentServiceLevelEnt = currentRS.getRsServiceLevelOrgEnterpriseName();
        }

        if (!FieldUtil.isNull(inputRS.getRsServiceLevelOrgName())) {
          inputServiceLevelOrg = inputRS.getRsServiceLevelOrgName();
        }
        if (!FieldUtil.isNull(currentRS.getRsServiceLevelOrgName())) {
          currentServiceLevelOrg = currentRS.getRsServiceLevelOrgName();
        }
      }

      //Comparison based on sysId.
      if (inputSysServiceLevelId == null) {
        if (currentSysServiceLevelId != null && origin != null
          && origin.equalsIgnoreCase(OriginEnum.UI.stringValue())) {
          isDiff = true;
        }
      }
      else {
        if (currentSysServiceLevelId == null) {
          isDiff = true;
        }
        else if (!inputSysServiceLevelId.equals(currentSysServiceLevelId)) {
          isDiff = true;
        }
      }

      //compare based on natural key
      if (inputServiceLevelName != null && inputServiceLevelEnt != null && inputServiceLevelOrg != null) {
        if (currentServiceLevelName == null || currentServiceLevelEnt == null || currentServiceLevelOrg == null) {
          isDiff = true;
        }
        else if (!(inputServiceLevelName.equalsIgnoreCase(currentServiceLevelName)
          && inputServiceLevelEnt.equalsIgnoreCase(currentServiceLevelEnt)
          && inputServiceLevelOrg.equalsIgnoreCase(currentServiceLevelOrg))) {
          isDiff = true;
        }
      }
      else if ((currentServiceLevelName != null || currentServiceLevelEnt != null || currentServiceLevelOrg != null)
        && (origin != null && origin.equalsIgnoreCase(OriginEnum.UI.stringValue()))) {
        isDiff = true;
      }
    }
    return isDiff;
  }

  public static boolean isContractPolicy(EnhancedOrder order, PlatformUserContext ctx)
    throws InstantiationException, IllegalAccessException {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "isContractPolicy")) {
      boolean returnValue = false;
      long itemId = -1;

      ItemRow itemRow = OMSUtil.getItem(
        new ItemKey(
          order.getValueChainId(),
          order.getOrderLines().get(0).getItemEnterpriseName(),
          order.getOrderLines().get(0).getItemName()));
      if (itemRow != null) {
        if (!FieldUtil.isNull(itemRow.getSccSysGenericItemId())) {
          ItemRow genericItem = OMSUtil.getItem(itemRow.getSccSysGenericItemId());
          if (genericItem != null)
            itemId = genericItem.getSysItemId();
        }
        if (itemId == -1) {
          itemId = itemRow.getSysItemId();
        }
      }

      SiteRow site = OMSUtil.getSite(order.getFirstRequestSchedule().getSysShipToSiteId(), (DvceContext) ctx);
      SiteRow parentSite = null;
      if (site.getSysParentSiteId() != null)
        parentSite = OMSUtil.getSite(site.getSysParentSiteId(), (DvceContext) ctx);
      if (parentSite == null) {
        parentSite = site;
      }
      Buffer buffer = TransactionCache.getBuffer(
        itemId,
        parentSite.getSysSiteId(),
        order.getFirstRequestSchedule().getSysShipToLocationId(),
        ctx);
      String policyName = null;

      if (buffer != null && OrderUtil.getOMSBufferMdfs(buffer) != null) {
        policyName = OrderUtil.getOMSBufferMdfs(buffer).getSourcingPolicyName();
      }
      if (policyName == null) {
        ItemRow item = OMSUtil.getItem(itemId);
        if (item != null) {
          policyName = item.getOmsSourcingPolicyName();
        }
      }
      if (policyName != null) {
        SourcingPolicyNameEnum policyNameEnum = SourcingPolicyNameEnum.get(policyName);
        if (policyNameEnum != null) {
          ContractSourcingPolicy sourcePolicy = ContractSourcingPolicyFactory.getInstance().getPolicy(
            policyNameEnum.intValue());
          if (sourcePolicy != null) {
            returnValue = true;
          }
        }
      }
      return returnValue;
    }
  }

  public static AclLine getAclLine(EnhancedOrder order, PlatformUserContext context) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getAclLine")) {
      List<Long> itemIds = new ArrayList<Long>();
      if (FieldUtil.isNull(order.getSysCustomerId())) {
        return null;
      }
      for (OrderLine line : order.getOrderLines()) {
        if (OrderUtil.nonTransitionalStates.contains(line.getState())) {
          continue;
        }
        if (!FieldUtil.isNull(line.getSysItemId())) {
          itemIds.add(line.getSysItemId());
        }
      }
      if (order.getSysSellingOrgId() == null) {
        OrganizationRow orgRow = OrganizationCacheManager.getInstance().getOrganization(
          new OrganizationKey(
            context.getValueChainId(),
            order.getSellingOrgEnterpriseName(),
            order.getSellingOrgName()));
        if (orgRow != null)
          order.setSysSellingOrgId(orgRow.getSysOrgId(), false);
      }
      if (!itemIds.isEmpty() && order.getSysSellingOrgId() != null) {
        for (long itemId : itemIds) {
          AclLine acl = TransactionCache.getAclLine(
            order.getSysSellingOrgId(),
            order.getSysBuyingOrgId(),
            order.getSysCustomerId(),
            itemId,
            order.getOrderType(),
            context);
          if (acl != null)
            return acl;
        }
      }
      return null;
    }
  }

  public static Map<Long,AclLine> getItemToAclLineMap(EnhancedOrder order, PlatformUserContext context) {
    Map<Long,AclLine> itemToACLLine=new HashMap<Long,AclLine>();
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getAclLine")) {
      List<Long> itemIds = new ArrayList<Long>();
      if (FieldUtil.isNull(order.getSysCustomerId())) {
        return null;
      }
      for (OrderLine line : order.getOrderLines()) {
        if (OrderUtil.nonTransitionalStates.contains(line.getState())) {
          continue;
        }
        if (!FieldUtil.isNull(line.getSysItemId())) {
          itemIds.add(line.getSysItemId());
        }
      }
      if (order.getSysSellingOrgId() == null) {
        OrganizationRow orgRow = OrganizationCacheManager.getInstance().getOrganization(
          new OrganizationKey(
            context.getValueChainId(),
            order.getSellingOrgEnterpriseName(),
            order.getSellingOrgName()));
        if (orgRow != null)
          order.setSysSellingOrgId(orgRow.getSysOrgId(), false);
      }
      if (!itemIds.isEmpty() && order.getSysSellingOrgId() != null) {
        for (long itemId : itemIds) {
          AclLine acl = TransactionCache.getAclLine(
            order.getSysSellingOrgId(),
            order.getSysBuyingOrgId(),
            order.getSysCustomerId(),
            itemId,
            order.getOrderType(),
            context);
          if (acl != null)
            itemToACLLine.put(itemId,acl);
        }
      }
      return itemToACLLine;
    }
  }

  
  public static void sendEmailsByBuyerPlanner(List<EnhancedOrder> orders, PlatformUserContext ctx, String actionName) {
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled())
        psrKey = PSRLogger.enter(PSR_ID + "sendEmailsByBuyerPlanner");
      DefaultingHashMap<String, List<EnhancedOrder>> ordersByPlanner = new DefaultingHashMap<String, List<EnhancedOrder>>(
        ArrayList.class);
      List<EnhancedOrder> defaultOrders = new ArrayList<EnhancedOrder>();
      for (EnhancedOrder order : orders) {
        String plannerCode = order.getPlannerCode();
        if (!FieldUtil.isNull(plannerCode)) {
          ordersByPlanner.get(plannerCode).add(order);
        }
        else {
          defaultOrders.add(order);
        }
      }

      for (List<EnhancedOrder> plannerOrderList : ordersByPlanner.values()) {
        DefaultingHashMap<String, List<EnhancedOrder>> ordersOrderByVendor = new DefaultingHashMap<String, List<EnhancedOrder>>(
          ArrayList.class);
        for (EnhancedOrder order : plannerOrderList) {
          if (!FieldUtil.isNull(order.getSysVendorId()))
            ordersOrderByVendor.get(order.getSysVendorId()).add(order);
          else {
            defaultOrders.add(order);
          }
        }
        for (List<EnhancedOrder> orderList : ordersOrderByVendor.values()) {
          AlertUtil.getInstance().generateAlertForEnhancedOrder((DvceContext) ctx, actionName, orderList);
        }
      }

      for (EnhancedOrder order : defaultOrders) {
        AlertUtil.getInstance().generateAlertForEnhancedOrderBuyer(
          (DvceContext) ctx,
          actionName,
          Collections.singletonList(order));
      }
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
  }

  public static void sendEmailsBySupplier(List<EnhancedOrder> orders, PlatformUserContext ctx, String actionName) {
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled())
        psrKey = PSRLogger.enter(PSR_ID + "sendEmailsBySupplier");
      final DefaultingHashMap<String, List<EnhancedOrder>> ordersByPlanner = new DefaultingHashMap<String, List<EnhancedOrder>>(
        ArrayList.class);
      final List<EnhancedOrder> defaultOrders = new ArrayList<EnhancedOrder>();
      for (EnhancedOrder order : orders) {
        boolean email = OrderUtil.isEmailVendorForOrder(order, (DvceContext) ctx);
        if (email || OrderUtil.isDeploymentOrder(order)) {
          final String plannerCode = order.getPlannerCode();
          if (!FieldUtil.isNull(plannerCode)) {
            ordersByPlanner.get(plannerCode).add(order);
          }
          else {
            defaultOrders.add(order);
          }
        }
      }
      for (List<EnhancedOrder> plannerOrderList : ordersByPlanner.values()) {
        AlertUtil.getInstance().generateAlertForEnhancedOrder((DvceContext) ctx, actionName, plannerOrderList);
      }
      for (EnhancedOrder order : defaultOrders) {
        AlertUtil.getInstance().generateAlertForEnhancedOrder(
          (DvceContext) ctx,
          actionName,
          Collections.singletonList(order));
      }
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
  }

  /**
   * Return AVL Key
   *
   * @param line
   * @return
   */
  public static String getAvlKey(RequestSchedule rs) {
    OrderLine line = rs.getParent();
    EnhancedOrder order = line.getParent();
    Long itemId = line.getSysItemId();
    Long buyingOrg = order.getSysBuyingOrgId();
    Long sellingOrg = order.getSysSellingOrgId();
    Long shipToSiteId = rs.getSysShipToSiteId();
    return itemId + "/" + buyingOrg + "/" + sellingOrg + "/" + shipToSiteId + "/" + order.getSysVendorId();
  }

  public static List<EnhancedOrder> getOrdersFromDB(
    String sqlGroupName,
    String sqlName,
    SqlParams params,
    PlatformUserContext ctx) {
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled())
        psrKey = PSRLogger.enter(PSR_ID + "getOrdersFromDB");
      ModelDataService mds = Services.get(ModelDataService.class);
      SqlService sqlService = Services.get(SqlService.class);
      SqlResult result = null;
      if (params == null) {
        result = sqlService.executeQuery(sqlService.lookupSqlDef(sqlGroupName, sqlName));
      }
      else {
        result = sqlService.executeQuery(sqlService.lookupSqlDef(sqlGroupName, sqlName), params);
      }
      List<EnhancedOrder> orders = new ArrayList<EnhancedOrder>();

      if (result != null && result.getRows() != null && !result.getRows().isEmpty()) {
        List<Long> orderIds = new ArrayList<Long>();
        for (SqlRow row : result.getRows()) {
          orderIds.add(row.getLongValue("SYS_ENHANCED_ORDER_ID"));
        }
        if (!orderIds.isEmpty()) {
          Map<Long, EnhancedOrder> dbOrders = mds.readByIds(EnhancedOrder.class, orderIds, ctx);
          if (dbOrders != null) {
            orders.addAll(dbOrders.values());
          }
        }
      }
      return orders;
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
  }
  
  public static List<Long> getOrderIdsFromDB(
    String sqlGroupName,
    String sqlName,
    SqlParams params,
    PlatformUserContext ctx) {
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled())
        psrKey = PSRLogger.enter(PSR_ID + "getOrdersFromDB");
      SqlService sqlService = Services.get(SqlService.class);
      List<Long> ids = new ArrayList<Long>();
      SqlQueryRequest req = null;
      if (params == null) {
        req = new SqlQueryRequest(sqlService.lookupSqlDef(sqlGroupName, sqlName));
      }
      else {
        req = new SqlQueryRequest(sqlService.lookupSqlDef(sqlGroupName, sqlName), params);
      }
      
      SqlResultHandler resultHandler =   new AbstractSqlResultHandler() {
        @Override
        public void processSqlRow(SqlRow row) throws com.ordermgmtsystem.platform.data.sql.SqlResultHandlerException {

          if (!row.isNull("SYS_ENHANCED_ORDER_ID")) {
            ids.add(row.getLongValue("SYS_ENHANCED_ORDER_ID"));
          }
        }
      };
      
      sqlService.executeQuery(req, resultHandler);
      return ids;
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
  }
  
  public static List<EnhancedOrder> getOrdersFromIds(
    List<Long> ids,
    PlatformUserContext ctx) {
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled())
        psrKey = PSRLogger.enter(PSR_ID + "getOrdersFromIds");
      ModelDataService mds = Services.get(ModelDataService.class);
      
      List<EnhancedOrder> orders = new ArrayList<EnhancedOrder>();

        if (!ids.isEmpty()) {
          Map<Long, EnhancedOrder> dbOrders = mds.readByIds(EnhancedOrder.class, ids, ctx);
          if (dbOrders != null) {
            orders.addAll(dbOrders.values());
          }
        }
        if(orders.isEmpty()) {
          return null;
        }
      return orders;
    }
    finally {
      if (PSRLogger.isEnabled()) {
        PSRLogger.exit(psrKey);
      }
    }
  }

  /**
   * Get AVL from RS transient field
   *
   * @param rs
   * @return
   */
  public static AvlLine getAVLFromRS(RequestSchedule rs, PlatformUserContext userContext, Long sysItemId) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getAVLFromRS")) {
      OrderLine line = rs.getParent();
      EnhancedOrder order = line.getParent();
      AvlLine avlLine = null;
      if (!order.isIsSpot()) {
        return TransactionCache.getAvlLine(
          sysItemId,
          order.getSysBuyingOrgId(),
          order.getSysSellingOrgId(),
          rs.getSysShipToSiteId(),
          order.getSysVendorId(),
          line.getSysProductGroupLevelId(),
          userContext);
      }
      return avlLine;
    }
  }

  /**
   * Get ACL from RS 
   *
   * @param rs
   * @return AclLine
   */
  public static AclLine getACLFromRS(RequestSchedule rs, PlatformUserContext userContext) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getACLFromRS")) {
      OrderLine line = rs.getParent();
      EnhancedOrder order = line.getParent();
      AclLine aclLine = null;
      if (!order.isIsSpot() && !OrderUtil.isDeploymentOrder(order)) {
        return TransactionCache.getAclLine(
          order.getSysSellingOrgId(), 
          order.getSysBuyingOrgId(), 
          line.getSysItemId(), 
          order.getOrderType(), 
          userContext);
      }
      return aclLine;
    }
  }

  /**
   * Get AVL from RS transient field
   *
   * @param rs
   * @return
   */
  public static AvlLine getAVLFromRS(RequestSchedule rs, PlatformUserContext userContext) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getAVLFromRS")) {
      OrderLine line = rs.getParent();
      EnhancedOrder order = line.getParent();
      AvlLine avlLine = null;
      if (!order.isIsSpot() && !OrderUtil.isDeploymentOrder(order)) {
        return TransactionCache.getAvlLine(
          line.getSysItemId(),
          order.getSysBuyingOrgId(),
          order.getSysSellingOrgId(),
          rs.getSysShipToSiteId(),
          order.getSysVendorId(),
          line.getSysProductGroupLevelId(),
          userContext);
      }
      return avlLine;
    }
  }
  
  /**
   * Get AVL from RS transient field
   *
   * @param rs
   * @return
   */
  public static Map<RequestSchedule,AvlLine> getAVLLineFromRS(List<RequestSchedule> rsList, PlatformUserContext userContext) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getAVLFromRS")) {
    	Map<RequestSchedule,AvlKey> avlKeys = new HashMap<RequestSchedule,AvlKey> ();
    for(RequestSchedule rs : rsList) {
      if(OrderUtil.nonTransitionalStates.contains(rs.getState())) {
        continue;
      }
    	 OrderLine line = rs.getParent();
         EnhancedOrder order = line.getParent();
         AvlKey avlKey = new AvlKey(line.getSysItemId(),
        		 order.getSysBuyingOrgId(), order.getSysSellingOrgId(), 
        		 rs.getSysShipToSiteId(), order.getSysVendorId(), line.getSysProductGroupLevelId(), userContext);
         avlKeys.put(rs, avlKey);
         
    }
    Map<RequestSchedule,AvlLine>  avlLines = TransactionCache.getAvlLine(avlKeys);
      
      return avlLines;
    }
  }
  
  
  public static Map<RequestSchedule,Buffer> getBufferPerRS(List<EnhancedOrder> orders, PlatformUserContext userContext) {
	    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getBufferPerRS")) {
	    	List<RequestSchedule> allRsList= new ArrayList<RequestSchedule>();
	    for(EnhancedOrder order : orders) {
	    	allRsList.addAll(OrderUtil.getAllRequestSchedules(order)); 
	         
	    }
	    Map<RequestSchedule, Buffer>  buffers = TransactionCache.getBufferPerRS(allRsList, userContext);
	      
	      return buffers;
	    }
	  }

  public static Map<RequestSchedule,Buffer> getBufferPerRS(EnhancedOrder order, PlatformUserContext userContext) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getBufferPerRS")) {
      List<RequestSchedule> allRsList= new ArrayList<RequestSchedule>();
      allRsList.addAll(OrderUtil.getAllRequestSchedules(order)); 
      Map<RequestSchedule, Buffer>  buffers = TransactionCache.getBufferPerRS(allRsList, userContext);
      return buffers;
    }
  }

  /**
   * Get Collaborative Fields based on ENHANCED_ORDER_FIELDS_FOR_COLLABORATION policy
   * 
   * @param order
   * @param ctx
   * @return String with comma separated field names
   */
  public static String getCollaborativeFields(EnhancedOrder order, PlatformUserContext ctx) {
    if (order.getSysOwningOrgId() == null) {
      OrganizationKey key = new OrganizationKey();
      key.setVcId(order.getValueChainId());
      key.setEntName(order.getOwningOrgEnterpriseName());
      key.setOrgName(order.getOwningOrgName());
      OrganizationRow orgRow = OrganizationCacheManager.getInstance().getRow(key, (DvceContext) ctx);
      order.setSysOwningOrgId(orgRow.getSysOrgId(), false);
    }
    StringJoiner joiner = new StringJoiner(",");
    joiner.add(TransactionCache.getOrgPolicy(
      OrgProcPolicyConstants.ENHANCED_ORDER_FIELDS_FOR_COLLABORATION,
      order.getSysOwningOrgId(),
      "",
      ctx));
    joiner.add(TransactionCache.getOrgPolicy(
      OMSConstants.Policies.ENABLE_NOTIFICATION_BY_COLLABORATION,
      order.getSysOwningOrgId(),
      "",
      ctx));
    return joiner.toString();
  }

  /**
   * Get Map which holds the info about ship from site,ship to site and collaboration allowed or not
   * 
   * @param order
   * @param ctx
 * @param forceContext 
   * @return map
   */
  public static Map<String, Object> checkDOEligibleeForCollaboration(EnhancedOrder order,String forceContext,  PlatformUserContext ctx) {
	  return checkDOEligibleeForCollaboration(order, forceContext, null, ctx);
  }
  
  @SuppressWarnings("unchecked")
  public static Map<String, Object> checkDOEligibleeForCollaboration(EnhancedOrder order,String forceContext, Boolean isHierarchyContext,  PlatformUserContext ctx) {
	  try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "checkDOEligibleeForCollaboration")) {
		  if (order.getTransientField("doCollaborationMap") != null) {
			  return (Map<String, Object>) order.getTransientField("doCollaborationMap");
		  }
		  RequestSchedule reqSch = order.getOrderLines().get(0).getRequestSchedules().get(0);
		  Long sysShipToOrgId = reqSch.getSysRsShipToOrgId();
		  Map<String, Object> collobarationMap = new HashMap<String, Object>();
		  if(FieldUtil.isNull(sysShipToOrgId) && !FieldUtil.isNull(reqSch.getSysShipToSiteId())) {
			  SiteRow site = SiteCacheManager.getInstance().getRow(reqSch.getSysShipToSiteId(), (DvceContext)ctx);
			  if(Objects.nonNull(site)){
				  sysShipToOrgId = site.getSysOrgId();
			  }
		  }
		  Long sysShipFromOrgId = reqSch.getDeliverySchedules().get(0).getSysDsShipFromOrgId();
		  if(FieldUtil.isNull(sysShipFromOrgId) && !FieldUtil.isNull(reqSch.getDeliverySchedules().get(0).getSysShipFromSiteId())) {
			  SiteRow site = SiteCacheManager.getInstance().getRow(reqSch.getDeliverySchedules().get(0).getSysShipFromSiteId(), (DvceContext)ctx);
			  if(Objects.nonNull(site)){
				  sysShipFromOrgId = site.getSysOrgId();
			  }
		  }
		  boolean doCollobaration = false;
		  boolean allowCollobaration = getOrderSitePolicy(order, Policy.ENABLE_COLLABORATION_FOR_DO, ctx);
		  collobarationMap.put("shipToSiteOrgId", sysShipToOrgId);
		  collobarationMap.put("shipFromSiteOrgId", sysShipFromOrgId);
		  if (allowCollobaration && Objects.nonNull(sysShipToOrgId) && !sysShipToOrgId.equals(sysShipFromOrgId)
				  && !FieldUtil.isNull(order.getSysBuyingOrgId()) && !order.getSysBuyingOrgId().equals(ctx.getRoleOrganizationId())) {
			  doCollobaration = true;
		  }
		  boolean isSeller = isSellerContext(ctx, order, false,isHierarchyContext);
		  boolean isBuyer = isBuyerContext(ctx, order,isHierarchyContext);
		  Boolean isOrchestratorWithDualContext = OrderUtil.isOrchestratorWithDualContext(ctx, order);
		  if(isOrchestratorWithDualContext) {
			  if(StringUtils.isNullOrBlank(forceContext) || ("Buyer Context").equals(forceContext)) {
				  isBuyer = true;
				  isSeller = false;
			  }else {
				  isSeller = true;
				  isBuyer = false;
			  }
		  }else {
			  if ((isBuyer && isSeller) || (!isBuyer && !isSeller)) {
				  if (ctx.isDerivedFrom(SCCConstants.RoleTypes.VENDOR_CSR)) {
					  isSeller = true;
					  isBuyer = false;
				  }
				  else {
					  isBuyer = true;
					  isSeller = false;
				  }
			  }
			  if ((OrderRestUtil.check3PLPartnerForOrder(order, ctx, isHierarchyContext)) && ctx.isDerivedFrom(SCCConstants.RoleTypes.VENDOR_CSR)) {
				  isSeller = true;
			  }
		  }
		  if(isHierarchyContext==null) {
			  isHierarchyContext = isHierarchyContext(ctx, order);
		  }
		  collobarationMap.put("allowCollaboration", allowCollobaration);
		  collobarationMap.put("isBuyerContext", isBuyer || isHierarchyContext);
		  collobarationMap.put("isSellerContext", isSeller);
		  collobarationMap.put("isOrchestrator", isOrchestratorWithDualContext);
		  collobarationMap.put("doCollaboration", doCollobaration);
		  order.setTransientField("doCollaborationMap", collobarationMap);
		  return collobarationMap;
	  }
  }

  public static Pair<OptionalDouble, OptionalDouble> getDeploymentOrderTolerancePolicy(
    EnhancedOrder order,
    PlatformUserContext ctx) {
    @SuppressWarnings("unchecked")
    Optional<Pair<OptionalDouble, OptionalDouble>> toleranceValues = (Optional<Pair<OptionalDouble, OptionalDouble>>) order.getTransientField(
      OMSConstants.Policies.QUANTITY_TOLERANCE_FOR_DO);
    if (toleranceValues == null) {
      toleranceValues = retrieveDeploymentOrderTolerancePolicy(order, ctx);
      order.setTransientField(OMSConstants.Policies.QUANTITY_TOLERANCE_FOR_DO, toleranceValues);
    }
    return toleranceValues.orElse(null);
  }

  private static Optional<Pair<OptionalDouble, OptionalDouble>> retrieveDeploymentOrderTolerancePolicy(
    EnhancedOrder order,
    PlatformUserContext ctx) {
    String tolerancePolicyValue = getOrderSitePolicy(order, Policy.QUANTITY_TOLERANCE_FOR_DO, ctx);
    if (Objects.isNull(tolerancePolicyValue))
      return Optional.empty();
    Optional<Pair<OptionalDouble, OptionalDouble>> toleranceValues = splitUsingDelimeter(tolerancePolicyValue, ",");
    return toleranceValues;
  }

  private static Optional<Pair<OptionalDouble, OptionalDouble>> splitUsingDelimeter(
    String tolerancePolicyValue,
    String delimeter) {
    String[] splitValues = tolerancePolicyValue.split(delimeter);
    if (splitValues.length != 2) {
      return Optional.empty();
    }

    Pair<OptionalDouble, OptionalDouble> toleranceValues = new Pair<>(
      parseDouble(splitValues[0]),
      parseDouble(splitValues[1]));
    return Optional.of(toleranceValues);
  }

  private static OptionalDouble parseDouble(String doubleAsString) {
    try {
      double value = Double.parseDouble(doubleAsString);
      return OptionalDouble.of(value);
    }
    catch (NumberFormatException ex) {
      return OptionalDouble.empty();
    }
  }

  public static Double coalesce(Double... d) {
    for (int i = 0; i < d.length; i++) {
      if (Objects.nonNull(d[i]) && d[i] > DvceConstants.EPSILON) {
        return d[i];
      }
    }
    return 0d;
  }

  public static Long coalesce(Long... d) {
    for (int i = 0; i < d.length; i++) {
      if (Objects.nonNull(d[i]) && d[i] > DvceConstants.EPSILON) {
        return d[i];
      }
    }
    return 0L;
  }
  
  public static String coalesce(String... d) {
	  for (int i = 0; i < d.length; i++) {
		  if (Objects.nonNull(d[i]) && !FieldUtil.isNull(d[i]) && !StringUtil.isNullOrBlank(d[i])) {
			  return d[i];
		  }
	  }
	  return QuantityUOM._EACH;
  }

  /**
   * Returns first date value
   *
   * @param d array of date values
   * @return first not null value   */
  public static Calendar coalesce(Calendar... date) {
    for (int i = 0; i < date.length; i++) {
      if (!FieldUtil.isNull(date[i])) {
        return date[i];
      }
    }
    return date[0];
  }

  public static boolean isFromIntegration(EnhancedOrder order) {
    List<String> integOrigins = Arrays.asList(
      OriginEnum.UIUPLOAD.stringValue(),
      OriginEnum.INTEG.stringValue(),
      OriginEnum.EDI.stringValue());
    return integOrigins.contains(order.getOrigin());
  }

  /**
   * Get Configured Order State to create auto shipment
   *
   * @param order
   * @param ctx
   * @return
   */
  public static Pair<String, Long> getAutoShipmentFlag(EnhancedOrder order, PlatformUserContext ctx) {
    Map<String, Object> values = fetchAutoCreateShipmentValueBasedOnOrderType(order, ctx);
    Pair<String, Long> orderStateLeadTimePair = new Pair<String, Long>(null, 0L);
    orderStateLeadTimePair.first = values == null
      ? null
      : (StringUtils.isNullOrBlank(values.get(ORDER_STATE)) ? null : values.get(ORDER_STATE).toString());
    orderStateLeadTimePair.second = values == null ? 0 : (Long) values.get(LEAD_TIME);
    return orderStateLeadTimePair;
  }

  /**
   * Get Configured Auto Shipment state to Call appropriate Action
   *
   * @param order
   * @param ctx
   * @return
   */
  public static String getPreASNStateFlag(EnhancedOrder order, PlatformUserContext ctx) {
    Map<String, Object> values = fetchAutoCreateShipmentValueBasedOnOrderType(order, ctx);
    return (values != null && values.get(PRE_ASN_STATE) != null) ? values.get(PRE_ASN_STATE).toString() : null;
  }

  /**
   * Method to fetch AutoCreateShipmentValue for Order based on OrderType
   * PO refers to AVL and VendorMaster
   * Spot PO refers to VendorMaster
   * SO refers to ACL and CustomerMaster
   * DO and RO are based on Org. Policy 
   * to determine AutoCreateShipment value
   *
   * @param order
   * @param ctx
   * @return
   */
  @SuppressWarnings("unchecked")
  public static Map<String, Object> fetchAutoCreateShipmentValueBasedOnOrderType(
    EnhancedOrder order,
    PlatformUserContext ctx) {
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled()) {
        psrKey = PSRLogger.enter(PSR_ID + " fetchAutoCreateShipmentValueBasedOnOrderType");
      }
      Map<String, Object> values = (Map<String, Object>) order.getTransientField(
        OmsConstants.AUTO_CREATE_SHIPMENT_VALUE_TRANSIENT_FIELD);
      if (values != null)
        return values;
      String autoCreateShipment = null;
      String autoShipmentState = null;
      long autoPreAsnLeadTime = 0;
      values = new HashMap<String, Object>();
      OrderTypeEnum orderType = OrderTypeEnum.get(order.getOrderType());
      if(OrderTypeEnum.BLANKET_ORDER == orderType) { 
        if( Objects.equals(order.getSysBuyingOrgId(),order.getSysSellingOrgId())) {
          orderType = OrderTypeEnum.DEPLOYMENT_ORDER;
        }
        else {
          orderType = OrderTypeEnum.PURCHASE_ORDER;
        }
      }
      if (OrderTypeEnum.PURCHASE_ORDER == orderType) {
        //-- [PURCHASE_ORDER type will use AVL or VendorMaster for autoShip value]
    	List<OrderLine> lines = new ArrayList<OrderLine>();
    	lines.addAll(order.getOrderLines());
    	EnhancedOrder dbOrder= DirectModelAccess.readById(EnhancedOrder.class, order.getSysId(), ctx); 
    	if(dbOrder!=null) { 
    		lines.clear();
    		lines.addAll(dbOrder.getOrderLines()); 
    	}
    	line: for (OrderLine line : lines) {
          if (!order.isIsSpot()) { // This condition is added to respect the Vendor master for auto-create ASN policy for SpotPO
            if (!FieldUtil.isNull(line.getSysItemId())) {
              for (RequestSchedule rs : line.getRequestSchedules()) {
                if (OrderUtil.nonTransitionalStates.contains(rs.getState())) {
                  continue;
                }
                AvlLine avlLine = OrderUtil.getAVLFromRS(rs, ctx);
                if (avlLine != null) {
                  AvlLineMDFs avlLineMDFs = avlLine.getMDFs(AvlLineMDFs.class);
                  if (!FieldUtil.isNull(avlLineMDFs.getAutoCreateShipment())) {
                    autoCreateShipment = autoCreateShipment == null
                      ? avlLineMDFs.getAutoCreateShipment()
                      : autoCreateShipment;
                    autoShipmentState = autoShipmentState == null ? avlLineMDFs.getPreASNState() : autoShipmentState;
                    if (autoCreateShipment != null && autoShipmentState != null) { //-- If found, break the loop at Line level
                      values = getAutoCreateShipmentVendorValue(order, ctx);
                      if (Objects.nonNull(values) && !values.isEmpty()) {
                        autoPreAsnLeadTime = (long) values.get(LEAD_TIME);
                      }
                      break line;
                    }
                  }
                }
              }
            }
          }
          if (autoCreateShipment == null || autoShipmentState == null) { //--This condition is valid for Spot-PO as well.
            values = getAutoCreateShipmentVendorValue(order, ctx);
            if (!values.isEmpty()) {
              autoPreAsnLeadTime = (long) values.get(LEAD_TIME);
            }
          }
        }
      }
      else if (OrderTypeEnum.SALES_ORDER == orderType) {
        //-- [SALES_ORDER type will use ACL or CustomerMaster for autoShip value]
        AclLine aclLine = OrderUtil.getAclLine(order, ctx);
        if (aclLine != null) {
          AclLineMDFs aclLineMDFs = aclLine.getMDFs(AclLineMDFs.class);
          autoCreateShipment = autoCreateShipment == null ? aclLineMDFs.getAutoCreateShipment() : autoCreateShipment;
          autoShipmentState = autoShipmentState == null ? aclLineMDFs.getPreASNState() : autoShipmentState;
          if (autoCreateShipment == null || autoShipmentState == null) { //-- Fetch value from Customer Master if not present on AclLine
            values = getAutoCreateShipmentCustomerValue(order);
            if(null != values) {
              autoPreAsnLeadTime = (long) values.get(LEAD_TIME);
            }
          }
          else {
            values = getAutoCreateShipmentCustomerValue(order);
            if(null != values) {
              autoPreAsnLeadTime = (long) values.get(LEAD_TIME);
            }
          }
        }
        else { //--This condition is valid for Spot-SO as well.
          values = getAutoCreateShipmentCustomerValue(order);
          if(null != values) {
            autoPreAsnLeadTime = (long) values.get(LEAD_TIME);
          }
        }
      }
      else { //-- Condition for DEPLOYMENT_ORDER and RETURN_ORDER (Since both are Org. Policy dependent for autoShip value)
        String autoCreateShipmentPolicy = null, autoShipmentStatePolicy = null, preASNLeadTimePolicy = null;
        switch (orderType) {
          case DEPLOYMENT_ORDER:
            autoCreateShipmentPolicy = OrgProcPolicyConstants.AUTO_CREATE_SHIPMENT_DO_POLICY;
            autoShipmentStatePolicy = OMSConstants.Policies.PRE_ASNSTATE_DO;
            preASNLeadTimePolicy = OMSConstants.Policies.AUTO_PRE_ASNLEAD_TIME_DO;

            break;
          case RETURN_ORDER:
            autoCreateShipmentPolicy = OrgProcPolicyConstants.AUTO_CREATE_SHIPMENT_RO_POLICY;
            autoShipmentStatePolicy = OMSConstants.Policies.PRE_ASNSTATE_RO;
            preASNLeadTimePolicy = OMSConstants.Policies.AUTO_PRE_ASNLEAD_TIME_RO;
            break;
        }
        if (autoCreateShipmentPolicy != null) {
          autoCreateShipment = OrgProcPolicyUtil.getStringValue(
            (DvceContext) ctx,
            order.getValueChainId(),
            order.getSysOwningOrgId(),
            autoCreateShipmentPolicy,
            null);
        }
        if (autoShipmentStatePolicy != null) {
          autoShipmentState = TransactionCache.getOrgPolicy(
            autoShipmentStatePolicy,
            order.getSysOwningOrgId(),
            ShipmentConstants.AWAITING,
            ctx);
        }
        if (preASNLeadTimePolicy != null) {
          String policyValue = TransactionCache.getOrgPolicy(
            preASNLeadTimePolicy,
            order.getSysOwningOrgId(),
            null,
            ctx);
          if (!StringUtils.isNullOrBlank(policyValue)) {
            try {
              JSONObject json = new JSONObject(policyValue);
              autoPreAsnLeadTime = convertLeadTimeToMillisFromDHM(json);
            }
            catch (JSONException e) {
              order.setError("OMS.enhancedOrder.invalidPolicyValue", preASNLeadTimePolicy);
              return values;
            }
          }
        }
      }
      
      if(null == values) {
        values = new HashMap<String, Object>();
      }
      
      values.put(ORDER_STATE, FieldUtil.isNull (autoCreateShipment) ? (null != values ? values.get(ORDER_STATE) : "") : autoCreateShipment);
      values.put(PRE_ASN_STATE, FieldUtil.isNull (autoShipmentState) ? (null != values ? values.get(PRE_ASN_STATE) : "") : autoShipmentState);
      values.put(LEAD_TIME, autoPreAsnLeadTime);
      if (LOG.isDebugEnabled()) {
        LOG.debug("AutoCreateShipment Value =" + autoCreateShipment + " and AutoSHipmentState = " + autoShipmentState);
      }
      if (!FieldUtil.isNull(autoCreateShipment))
        order.setTransientField(OmsConstants.AUTO_CREATE_SHIPMENT_VALUE_TRANSIENT_FIELD, values);

      return values;
    }
    finally {
      if (PSRLogger.isEnabled())
        PSRLogger.exit(psrKey);
    }
  }

  public static Long convertLeadTimeToMillisFromDHM(JSONObject policyJSON) {
	  Long leadTimeInMillis = null;
	  try {
		  TreeMap<String, Object> map = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
		  map.putAll(policyJSON.toMap());
		  Integer leadTimeHour = map.get(OmsConstants.LEAD_TIME_HOUR) == null
				  ? 0
						  : (Integer) map.get(OmsConstants.LEAD_TIME_HOUR);
		  Integer leadTimeDay = map.get(OmsConstants.LEAD_TIME_DAY) == null
				  ? 0
						  : (Integer) map.get(OmsConstants.LEAD_TIME_DAY);
		  Integer leadTimeMinute = map.get(OmsConstants.LEAD_TIME_MINUTE) == null
				  ? 0
						  : (Integer) map.get(OmsConstants.LEAD_TIME_MINUTE);
		  leadTimeInMillis = leadTimeDay * DvceConstants.DAY + leadTimeHour * DvceConstants.HOUR
				  + leadTimeMinute * DvceConstants.MINUTE;
	  }catch(Exception e) {
		  LOG.error("Auto pre asn policy is not set in the proper format "+policyJSON.toString());
	  }
	  return leadTimeInMillis;
  }

  public static String getASNActionNameFromAutoShipmentState(EnhancedOrder order, PlatformUserContext ctx) {
    String autoShipmentState = getPreASNStateFlag(order, ctx);
    String actionName = com.ordermgmtsystem.oms.mpt.ShipmentConstants.Actions.CREATE_AND_CONFIRM;
    if (!FieldUtil.isNull(autoShipmentState) && ShipmentConstants.DRAFT.equals(autoShipmentState)) {
      actionName = com.ordermgmtsystem.oms.mpt.ShipmentConstants.Actions.CREATE;
    }
    return actionName;
  }

  public static Map<String, Object> getAutoCreateShipmentVendorValue(EnhancedOrder order, PlatformUserContext context) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getAutoCreateShipmentVendorValue")) {
      Map<String, Object> values = new HashMap<String, Object>();
      PartnerRow partnerRow = null;
      //-- try to fetch vendor if sysId is available on order
      if (!FieldUtil.isNull(order.getSysVendorId())) {
        partnerRow = PartnerUtil.getPartner(order.getSysVendorId(), context);
      }
      else {
        PartnerKey partnerKey = new PartnerKey(
          order.getVendorName(),
          order.getValueChainId(),
          order.getVendorEnterpriseName(),
          order.getVendorOrganizationName());
        partnerRow = PartnerUtil.getPartner(partnerKey, context);
      }
      //-- If valid Partner Row is returned from either of above cases, fetch the autoCreateShipmentValue from the row
      if (partnerRow != null) {
        if (partnerRow.isIsActive()) {
          values.put(ORDER_STATE, partnerRow.getOmsAutoCreateShipment());
          values.put(PRE_ASN_STATE, partnerRow.getOmsPreAsnstate());
          values.put(
            LEAD_TIME,
            partnerRow.getOmsAutoPreAsnleadTime() == null ? 0 : partnerRow.getOmsAutoPreAsnleadTime());
        }
        else {
          order.setError("OMS.enhancedOrder.InactivePartnerShip", new Object[] { partnerRow.getPartnerName() });
        }
      }
      return values;
    }
  }

  public static Map<String, Object> getAutoCreateShipmentCustomerValue(EnhancedOrder order) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getAutoCreateShipmentCustomerValue")) {
      if (order == null)
        return null;
      Map<String, Object> values = new HashMap<String, Object>();

      if (!FieldUtil.isNull(order.getSysCustomerId())) {
        PartnerRow row = PartnerUtil.getPartner(order.getSysCustomerId(), null);
        if (row != null) {
          values.put(ORDER_STATE, row.getOmsAutoCreateShipment());
          values.put(PRE_ASN_STATE, row.getOmsPreAsnstate());
          values.put(LEAD_TIME, row.getOmsAutoPreAsnleadTime() == null ? 0 : row.getOmsAutoPreAsnleadTime());
          return values;
        }
        return null;
      }
      else if (!FieldUtil.isNull(order.getCustomerEnterpriseName())
        && !FieldUtil.isNull(order.getCustomerOrganizationName()) && !FieldUtil.isNull(order.getCustomerName())) {
        PartnerKey key = new PartnerKey();
        key.setVcId(order.getValueChainId());
        key.setEntName(order.getCustomerEnterpriseName());
        key.setOrgName(order.getCustomerOrganizationName());
        key.setPartnerName(order.getCustomerName());
        PartnerRow row = PartnerUtil.getPartner(key, null);
        if (row != null) {
          values.put(ORDER_STATE, row.getOmsAutoCreateShipment());
          values.put(PRE_ASN_STATE, row.getOmsPreAsnstate());
          values.put(LEAD_TIME, row.getOmsAutoPreAsnleadTime() == null ? 0 : row.getOmsAutoPreAsnleadTime());
          return values;
        }
        return null;
      }
      else {
        Long tempBuyingOrgId = getOrgByKeyOptionally(
          order.getSysBuyingOrgId(),
          order.getBuyingOrgEnterpriseName(),
          order.getBuyingOrgName(),
          order.getValueChainId());
        Long tempSellingOrgId = getOrgByKeyOptionally(
          order.getSysSellingOrgId(),
          order.getSellingOrgEnterpriseName(),
          order.getSellingOrgName(),
          order.getValueChainId());

        PartnerRow row = PartnerUtil.getCustomerMasterRow(order.getValueChainId(), tempSellingOrgId, tempBuyingOrgId);
        if (row != null) {
          values.put(ORDER_STATE, row.getOmsAutoCreateShipment());
          values.put(PRE_ASN_STATE, row.getOmsPreAsnstate());
          values.put(LEAD_TIME, row.getOmsAutoPreAsnleadTime() == null ? 0 : row.getOmsAutoPreAsnleadTime());
          return values;
        }
      }
      return null;
    }
  }

  public static Long getOrgByKeyOptionally(Long tempBuyingOrgId, String entName, String orgName, Long vcId) {
    if (!FieldUtil.isNull(tempBuyingOrgId)) {
      return tempBuyingOrgId;
    }
    if (!FieldUtil.isNull(entName) && !FieldUtil.isNull(orgName) && !FieldUtil.isNull(vcId)) {
      OrganizationKey key = new OrganizationKey();
      key.setVcId(vcId);
      key.setEntName(entName);
      key.setOrgName(orgName);
      OrganizationRow orgRow = OrganizationCacheManager.getInstance().getRow(key, null);
      if (orgRow == null) {
        return null;
      }
      else {
        return orgRow.getSysOrgId();
      }
    }
    return null;
  }

  /**
   * Check if Order is Sales Order
   *
   * @param enhancedOrder
   * @return
   */
  public static boolean isSalesOrder(EnhancedOrder order) {
    return OrderTypeEnum.SALES_ORDER.stringValue().equals(order.getOrderType());
  }

  /**
   * 
   * Filter mutable/changeable list of orders
   * to include only those that have been updated;
   *
   * @param Current Orders
   * @param Orders (current) before input merge and domain services updates
   */
  public static void filterForUpdatedOrders(List<EnhancedOrder> orders, List<EnhancedOrder> prevCurrentOrders) {
    List<String> fieldChangesToExclude = Arrays.asList(
      "VendorRevisionNumber",
      "LatestVendorRevisionDate",
      "AuditId",
      "LnAuditId",
      "RsAuditId",
      "DsAuditId",
      "LnAuditId",
      "RsAuditId",
      "DsAuditId",
      "OMS.Edi865Code");

    ModelDiffConfig diffConfig = new ModelDiffConfig();
    diffConfig.setIgnoreCoreFieldChanges(true);
    diffConfig.setIgnoreNullModel1Fields(true);
    diffConfig.setRecurseChildLevels(true);

    for (Iterator<EnhancedOrder> orderIter = orders.iterator(); orderIter.hasNext();) {
      EnhancedOrder order = orderIter.next();
      EnhancedOrder prevOrder = ModelUtil.findMatching(order, prevCurrentOrders);
      if (Objects.nonNull(prevOrder)) {
        List<Difference> diffs = ModelDiffUtil.diff(prevOrder, order, diffConfig); //PrevOrder, CurrentOrder
        for (Iterator<Difference> diffIter = diffs.iterator(); diffIter.hasNext();) {
          Difference diff = diffIter.next();
          if (fieldChangesToExclude.contains(diff.getFieldName())) {
            diffIter.remove();
          }
        }
        if (diffs.size() == 0) {
          orderIter.remove(); //Order not updated, remove.
        }
      }
    }
  }

  /**
   * 
   * Clones an order
   *  - deep cloning
   *  
   * @param orderToClone
   * @return Cloned Order or null (if order was not Serializable)
   */
  public static EnhancedOrder getClonedOrder(EnhancedOrder orderToClone) {
    List<EnhancedOrder> orders = getClonedOrderList(Collections.singletonList(orderToClone));
    if (orders.size() > 0)
      return orders.get(0);
    else
      return null;
  }

  /**
   * 
   * Clones a list of orders
   *
   * @param ordersToClone
   * @return Cloned List Of Orders (could be empty if order was not Serializable)
   */
  public static List<EnhancedOrder> getClonedOrderList(List<EnhancedOrder> ordersToClone) {
    List<EnhancedOrder> clonedListOfOrders = new ArrayList<EnhancedOrder>();
    
    try {
      
      for (EnhancedOrder order : ordersToClone) {
        // clonedListOfOrders.add((EnhancedOrder) SerializationUtils.clone(order));
        
        // PDS-20917 - Workaround for PLT issue /w ClassLoader in 24.0; issue not in 23.0;
        // SERIALIZE Object
        byte[] serializedEO = SerializationUtils.serialize((Serializable) order);
        
        // DESERIALIZE Object
        InputStream inputStream = null;
        inputStream = new ByteArrayInputStream(serializedEO);
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        EnhancedOrder clonedEO = (EnhancedOrder) objectInputStream.readObject();
        clonedListOfOrders.add(clonedEO);
        objectInputStream.close();
        inputStream.close();
      }
      
    }
    catch (SerializationException e) {
      LOG.error("Cloning of order failed. Order is not serializable", e);
    } catch(IOException e) {
      LOG.error("Cloning of order failed. IOException - Order not deserialized", e);
    } catch(ClassNotFoundException e) {
      LOG.error("Cloning of order failed. ClassNotFound", e);
    }

    return clonedListOfOrders;
  }

  /**
   * set vendor approval dates
   *
   * @param inputOrder
   * @param ctx
   */
  public static void setVendorApprovalDates(EnhancedOrder order, PlatformUserContext ctx) {
    order.setVendorApprovedByEnterpriseName(ctx.getUserEnterpriseName());
    order.setVendorApprovedByName(ctx.getUserName());
    order.setVendorOrderApprovalDate(Calendar.getInstance());
  }

  public static String getPath(EnhancedOrder order) {
    return order.getOrderNumber();
  }

  public static String getPath(OrderLine line) {
    return getPath(line.getParent()) + "/" + line.getLineNumber();
  }

  public static String getPath(RequestSchedule rs) {
    return getPath(rs.getParent()) + "/" + rs.getRequestScheduleNumber();
  }

  public static String getPath(DeliverySchedule ds) {
    return getPath(ds.getParent()) + "/" + ds.getDeliveryScheduleNumber();
  }

  /**
   * This method will be called before moving delivery states to VCWC ie. Delivery Schedule is updated. This method will set an 
   * error if  ds which is in FullFillment state is updated.
   *
   * @param ds
   * @return
   */
  public static boolean validateDeliveryScheduleForFulfillmentStates(DeliverySchedule ds) {
    EnhancedOrder order = ds.getParent().getParent().getParent();
    if (ds != null && OrderUtil.inFullFillmentStates.contains(ds.getState())) {
      if (!order.isSetError()) {
        ds.setError("OMS.enhancedOrder.cannotUpdateDeliverySchedule");
      }
      return false;
    }
    return true;
  }

  public static void copyCurrentOrderRequestFieldsToInput(
    EnhancedOrder currentOrder,
    EnhancedOrder inputOrder,
    PlatformUserContext ctx) {
    //Buyer Code, Planner Notes,Planner Code
    inputOrder.setBuyerCode(currentOrder.getBuyerCode());
    inputOrder.setPlannerNotes(currentOrder.getPlannerNotes());
    inputOrder.setPlannerCode(currentOrder.getPlannerCode());
    //Service Level
    inputOrder.setServiceLevelName(currentOrder.getServiceLevelName());
    inputOrder.setServiceLevelOrgEnterpriseName(currentOrder.getServiceLevelOrgEnterpriseName());
    inputOrder.setServiceLevelOrgName(currentOrder.getServiceLevelOrgName());
    inputOrder.setSysServiceLevelId(currentOrder.getSysServiceLevelId(), false);
    //OMO,WMS Order NO
    if (OrderTypeEnum.DEPLOYMENT_ORDER.stringValue().equals(inputOrder.getOrderType())
      && isDOCollaborationEnabled(inputOrder, ctx)) {
      inputOrder.setOmoOrderNumber(currentOrder.getOmoOrderNumber()); // add check of DO and Collab and check.
      inputOrder.setWmsOrderNumber(currentOrder.getWmsOrderNumber());
    }
    //Order Description,Priority
    inputOrder.setOrderDescription(currentOrder.getOrderDescription());
    inputOrder.setOrderPriority(currentOrder.getOrderPriority());
    //Buying,ShipFrom,ShipTo ,OMO  Org and fullfilment org
    inputOrder.setBuyingOrgName(currentOrder.getBuyingOrgName());
    inputOrder.setBuyingOrgEnterpriseName(currentOrder.getBuyingOrgEnterpriseName());
    inputOrder.setSysBuyingOrgId(currentOrder.getSysBuyingOrgId(), false);
    inputOrder.setShipFromOrgName(currentOrder.getShipFromOrgName());
    inputOrder.setShipFromOrgEnterpriseName(currentOrder.getShipFromOrgEnterpriseName());
    inputOrder.setSysShipFromOrgId(currentOrder.getSysShipFromOrgId(), false);
    inputOrder.setShipToOrgEnterpriseName(currentOrder.getShipToOrgEnterpriseName());
    inputOrder.setShipToOrgName(currentOrder.getShipToOrgName());
    inputOrder.setSysShipToOrgId(currentOrder.getSysShipToOrgId(), false);
    inputOrder.setOrderMgmtOrgEnterpriseName(currentOrder.getOrderMgmtOrgEnterpriseName());
    inputOrder.setOrderMgmtOrgName(currentOrder.getOrderMgmtOrgName());
    inputOrder.setSysOrderMgmtOrgId(currentOrder.getSysOrderMgmtOrgId(), false);

    //Currency, Inco Term , Owning Site , Total Amount and BPO no.
    inputOrder.setCurrency(currentOrder.getCurrency());
    inputOrder.setIncoTerms(currentOrder.getIncoTerms());
    inputOrder.setOwningSiteEnterpriseName(currentOrder.getOwningSiteEnterpriseName());
    inputOrder.setOwningSiteName(currentOrder.getOwningSiteName());
    inputOrder.setOwningSiteOrganizationName(currentOrder.getOwningSiteOrganizationName());
    inputOrder.setSysOwningSiteId(currentOrder.getSysOwningSiteId(), false);
    inputOrder.setTotalAmount(currentOrder.getTotalAmount());
    inputOrder.setBPONumber(currentOrder.getBPONumber());

    //reset buyer flags
    inputOrder.setIsSpot(currentOrder.isIsSpot());
    inputOrder.setIsConsignment(currentOrder.isIsConsignment());
    inputOrder.setIsVMI(currentOrder.isIsVMI());
    inputOrder.setIsAutoReceipt(currentOrder.isIsAutoReceipt());
    inputOrder.setIsEmergency(currentOrder.isIsEmergency());
    inputOrder.setIsExpedite(currentOrder.isIsExpedite());
    inputOrder.setIsPromotion(currentOrder.isIsPromotion());

  }

  public static void copyCurrentOrderLineRequestFieldsToInput(OrderLine currentOrderLine, OrderLine inputOrderLine) {
    //Item
    inputOrderLine.setSysItemId(currentOrderLine.getSysItemId(), false);
    inputOrderLine.setItemEnterpriseName(currentOrderLine.getItemEnterpriseName());
    inputOrderLine.setItemName(currentOrderLine.getItemName());
    //Line Type
    inputOrderLine.setLineType(currentOrderLine.getLineType());
    inputOrderLine.setLineTypeDesc(currentOrderLine.getLineTypeDesc());
    //Unit Price
    inputOrderLine.setUnitPrice(currentOrderLine.getUnitPrice());
    //Line Amount
    inputOrderLine.setLineAmount(currentOrderLine.getLineAmount());
    //BPO Line Number
    inputOrderLine.setBPOLineNumber(currentOrderLine.getBPOLineNumber());
    //EXT Commodity Code
    inputOrderLine.setExtCommodityCode(currentOrderLine.getExtCommodityCode());
    //HTS code
    inputOrderLine.setHTSCode(currentOrderLine.getHTSCode());
    //Singleton Number
    inputOrderLine.setSingletonNumber(currentOrderLine.getSingletonNumber());
    inputOrderLine.setSingletonEnterpriseName(currentOrderLine.getSingletonEnterpriseName());
    //Line weight,volume 
    inputOrderLine.setLineWeightAmount(currentOrderLine.getLineWeightAmount());
    inputOrderLine.setLineWeightUOM(currentOrderLine.getLineWeightUOM());
    inputOrderLine.setLineVolumeAmount(currentOrderLine.getLineVolumeAmount());
    inputOrderLine.setLineVolumeUOM(currentOrderLine.getLineVolumeUOM());
    //Request Qty
    inputOrderLine.setLineTotalRequestQtyAmount(currentOrderLine.getLineTotalRequestQtyAmount());
    inputOrderLine.setLineTotalRequestQtyUOM(currentOrderLine.getLineTotalRequestQtyUOM());
    if (!FieldUtil.isNull(inputOrderLine.getExtItemName()) && !FieldUtil.isNull(currentOrderLine.getSysItemId())) {
      inputOrderLine.setExtItemName(DvceConstants.NULL_STRING_VALUE);
    }
  }

  public static void copyCurrentRequestScheduleRequestFieldsToInput(
    RequestSchedule currentRequestSchedule,
    RequestSchedule inputRequestSchedule) {
    //Ship To Site/ Address
    inputRequestSchedule.setShipToAddress(currentRequestSchedule.getShipToAddress());
    inputRequestSchedule.setShipToLocationName(currentRequestSchedule.getShipToLocationName());
    inputRequestSchedule.setShipToLocationSiteEnterpriseName(
      currentRequestSchedule.getShipToLocationSiteEnterpriseName());
    inputRequestSchedule.setShipToLocationSiteName(currentRequestSchedule.getShipToLocationSiteName());
    inputRequestSchedule.setShipToLocationSiteOrganizationName(
      currentRequestSchedule.getShipToLocationSiteOrganizationName());
    inputRequestSchedule.setShipToSiteEnterpriseName(currentRequestSchedule.getShipToSiteEnterpriseName());
    inputRequestSchedule.setShipToSiteName(currentRequestSchedule.getShipToSiteName());
    inputRequestSchedule.setShipToSiteOrganizationName(currentRequestSchedule.getShipToSiteOrganizationName());
  }

  public static void copyCurrentDeliveryScheduleRequestFieldsToInputDeliverySchedule(
    DeliverySchedule currentDeliverySchedule,
    DeliverySchedule inputDeliverySchedule,
    PlatformUserContext ctx) {
    if (AbstractOrderCollaboration.isCollabPerDS(inputDeliverySchedule, (DvceContext) ctx)) {
      return;
    }
    DeliveryScheduleMDFs inputOmsDsMdfs = inputDeliverySchedule.getMDFs(DeliveryScheduleMDFs.class);
    DeliveryScheduleMDFs currentOmsDsMdfs = currentDeliverySchedule.getMDFs(DeliveryScheduleMDFs.class);
    //Request Qty
    inputDeliverySchedule.setRequestQuantity(currentDeliverySchedule.getRequestQuantity());
    //RDD
    inputDeliverySchedule.setRequestDeliveryDate(currentDeliverySchedule.getRequestDeliveryDate());
    //Request Unit Price and UOM
    if (currentDeliverySchedule.isSetRequestUnitPriceAmount()) {
      inputDeliverySchedule.setRequestUnitPriceAmount(currentDeliverySchedule.getRequestUnitPriceAmount());
      inputDeliverySchedule.setRequestUnitPriceUOM(currentDeliverySchedule.getRequestUnitPriceUOM());
    }
    else {
      inputDeliverySchedule.setRequestUnitPriceAmount(DvceConstants.NULL_DOUBLE_VALUE);
      inputDeliverySchedule.setRequestUnitPriceUOM(DvceConstants.NULL_STRING_VALUE);
    }
    if (currentDeliverySchedule.isSetRequestUnitPriceAmount()) {
      inputDeliverySchedule.setRequestUnitPriceAmount(currentDeliverySchedule.getRequestUnitPriceAmount());
      inputDeliverySchedule.setRequestUnitPriceUOM(currentDeliverySchedule.getRequestUnitPriceUOM());
    }
    else {
      inputDeliverySchedule.setRequestUnitPriceAmount(DvceConstants.NULL_DOUBLE_VALUE);
      inputDeliverySchedule.setRequestUnitPriceUOM(DvceConstants.NULL_STRING_VALUE);
    }
    if (currentOmsDsMdfs.isSetRequestPricePer()) {
      inputOmsDsMdfs.setRequestPricePer(currentOmsDsMdfs.getRequestPricePer());
    }
    else {
      inputOmsDsMdfs.setRequestPricePer(DvceConstants.NULL_DOUBLE_VALUE);
    }
  }

  /**
   * This method will return coalesce value considering zero value
   * 
   * @param ds
   * @return
   */
  public static double getQuantityCoalesce(DeliverySchedule ds) {
    return ds.isSetAgreedQuantity() && !FieldUtil.isNull(ds.getAgreedQuantity())
      ? ds.getAgreedQuantity()
      : (ds.isSetPromiseQuantity() && !FieldUtil.isNull(ds.getPromiseQuantity())
        ? ds.getPromiseQuantity()
        : (ds.isSetRequestQuantity() && !FieldUtil.isNull(ds.getRequestQuantity()) ? ds.getRequestQuantity() : 0.0));
  }
  
  /**
   * This method will return coalesce value ignoring zero value
   * 
   * @param ds
   * @return
   */
  public static double getQuantityCoalesceIgnoringZero(DeliverySchedule ds) {
    return ds.isSetAgreedQuantity() && !FieldUtil.isNull(ds.getAgreedQuantity()) && ds.getAgreedQuantity() !=0
      ? ds.getAgreedQuantity()
      : (ds.isSetPromiseQuantity() && !FieldUtil.isNull(ds.getPromiseQuantity()) && ds.getPromiseQuantity() !=0
        ? ds.getPromiseQuantity()
        : (ds.isSetRequestQuantity() && !FieldUtil.isNull(ds.getRequestQuantity()) ? ds.getRequestQuantity() : 0.0));
  }

  
  public static double getQuantityCoalesceBuyerReject(DeliverySchedule ds) {
	    return ds.isSetAgreedQuantity() && !FieldUtil.isNull(ds.getAgreedQuantity())
	      ? ds.getAgreedQuantity()
	        : (ds.isSetRequestQuantity() && !FieldUtil.isNull(ds.getRequestQuantity()) ? ds.getRequestQuantity() : 0.0);
	  }
  /** Checks the AVL line and determines, if the VMI needs approval.
  * This flag will also be used to determine if order should be collaborated with Buyer or it should be directly moved Open.
  * 
  * @param eo
  * @param context
  * @return
  */
  public static boolean isVMIAuthorizationRequired(EnhancedOrder eo, Map<RequestSchedule,AvlLine> avlLines, PlatformUserContext context) throws Exception {
    boolean isAvlFound = false;
    if (!eo.isIsVMI()) {
      return true;
    }
    for (OrderLine line : eo.getOrderLines()) {
      for (RequestSchedule rs : line.getRequestSchedules()) {
        AvlLine avlLine = null;
        if(avlLines == null) {
        	avlLine = OrderUtil.getAVLFromRS(rs, context);
        } else {
        	avlLine= avlLines.get(rs);
        }
        if (avlLine != null) {
          isAvlFound = true;
          AvlLineMDFs avlMDFs = avlLine.getMDFs(AvlLineMDFs.class);
          if (avlMDFs.isRequireVMIOrderApproval())
            return avlMDFs.isRequireVMIOrderApproval();
        }
      }
    }

    if (!isAvlFound) {
      LOG.error(
        "No avls found for Enhanced Order" + eo.getSysBuyingOrgId() + ", selling org:" + eo.getSysSellingOrgId());
      throw new Exception("No AVL found");
    }

    return false;
  }

  public static com.ordermgmtsystem.supplychaincore.model.BufferDetailMDFs getSCCBufferMdfs(Buffer buffer) {
    if (buffer == null) {
      return null;
    }
    return buffer.getMDFs(com.ordermgmtsystem.supplychaincore.model.BufferDetailMDFs.class);
  }

  public static com.ordermgmtsystem.oms.model.BufferDetailMDFs getOMSBufferMdfs(Buffer buffer) {
    if (buffer == null) {
      return null;
    }
    return buffer.getMDFs(com.ordermgmtsystem.oms.model.BufferDetailMDFs.class);
  }

  public static Pair<Long, String> getPackageItemFromAVL(AvlLine avlLine) {
    if (Objects.nonNull(avlLine)) {
      AvlLineMDFs avlLineMdf = avlLine.getMDFs(AvlLineMDFs.class);
      if (Objects.nonNull(avlLineMdf) && Objects.nonNull(avlLineMdf.getSysPackageItemId()))
        return new Pair<Long, String>(avlLineMdf.getSysPackageItemId(), avlLineMdf.getPackageItemName());
    }
    return null;
  }

  public static Pair<Long, String> getPackageItemFromBuffer(Buffer bufferRow) {
    if (Objects.nonNull(bufferRow)) {
      BufferDetailMDFs bufferMdf = OrderUtil.getOMSBufferMdfs(bufferRow);
      if (Objects.nonNull(bufferMdf) && Objects.nonNull(bufferMdf.getSysPackageItemId()))
        return new Pair<Long, String>(bufferMdf.getSysPackageItemId(), bufferMdf.getPackageItemName());
    }
    return null;
  }

  public static Pair<Long, String> getPackageItemFromItem(Long itemId) {
    if (!FieldUtil.isNull(itemId)) {
      ItemRow itemRow = ItemCacheManager.getInstance().getItem(itemId);
      if (Objects.nonNull(itemRow) && !FieldUtil.isNull(itemRow.getOmsSysPackageItemId())) {
        ItemRow packageItemRow = ItemCacheManager.getInstance().getItem(itemId);
        if (Objects.nonNull(packageItemRow) && !FieldUtil.isNull(packageItemRow.getOmsSysPackageItemId()))
          return new Pair<Long, String>(packageItemRow.getOmsSysPackageItemId(), packageItemRow.getItemName());
      }
    }
    return null;
  }

  /**
   * set buyer approval dates
   *
   * @param inputOrder
   * @param ctx
   */
  public static void setBuyerApprovalDates(EnhancedOrder inputOrder, PlatformUserContext ctx) {
    Calendar today = Calendar.getInstance();
    inputOrder.setVendorNotificationDate(today);
    inputOrder.setBuyerOrderApprovalDate(today);
    if (!inputOrder.isIsVMI()) {
      inputOrder.setBuyerApprovedByName(ctx.getUserName());
      inputOrder.setBuyerApprovedByEnterpriseName(ctx.getUserEnterpriseName());
    }
  }

  public static void validateAgents(
    Model model,
    List<String> agentFieldNames,
    List<String> otherOrgFieldsToCompare,
    String fromOrg,
    boolean isBuyer,
    DvceContext dvceContext) {
    LevelMappingHelper levelHelper = LevelMappingHelper.get(model.getModelLevelType());
    Map<String, Long> agentsMap = new HashMap<String, Long>();
    if (Objects.nonNull(agentFieldNames) && !agentFieldNames.isEmpty()) {
      if (Objects.nonNull(otherOrgFieldsToCompare) && !otherOrgFieldsToCompare.isEmpty()) {
        agentFieldNames.addAll(otherOrgFieldsToCompare);
      }
      agentFieldNames.stream().forEach(fieldName -> {
        ModelLinkMappingHelper mapping = levelHelper.getModelLinkMapping(fieldName);
        if (Objects.nonNull(mapping) && Objects.nonNull(mapping.getMapping().getSysIdFieldValue(model))
          && !FieldUtil.isNull(mapping.getMapping().getSysIdFieldValue(model))) {
          agentsMap.put(fieldName, mapping.getMapping().getSysIdFieldValue(model));
        }
      });
    }

    if (Objects.nonNull(agentsMap) && !agentsMap.isEmpty()) {
      if (Objects.nonNull(fromOrg)) {
        List<Long> notValidPartner = agentsMap.values().stream().filter(
          agentId -> (Objects.nonNull(agentId) && Objects.isNull(
            OMSUtil.getAgentPartnerRow(
              dvceContext.getValueChainId(),
              fromOrg,
              OrganizationCacheManager.getInstance().getOrganization(agentId).getOrgName(),
              isBuyer)))).collect(Collectors.toList());
        if (Objects.nonNull(notValidPartner) && !notValidPartner.isEmpty()) {
          HashSet<String> errorOrgs = new HashSet<String>();
          notValidPartner.stream().forEach(agentId -> {
            errorOrgs.add(OrganizationCacheManager.getInstance().getOrganization(agentId).getOrgName());
          });
          model.setError(
            StringUtils.collectionToDelimitedString(errorOrgs, ",") + " orgs dont have proper partner ship with "
              + fromOrg);
          return;
        }
      }
      List<String> duplicateOrgs = agentsMap.keySet().stream().filter(
        agentKey -> Collections.frequency(agentsMap.values(), agentsMap.get(agentKey)) > 1).collect(
          Collectors.toList());
      if (Objects.nonNull(duplicateOrgs) && !duplicateOrgs.isEmpty()) {
        model.setError("Duplicate orgs found in agents " + StringUtils.collectionToDelimitedString(duplicateOrgs, ","));
        return;
      }
    }
  }

  public static List<EnhancedOrder> isAnyChildInTargetState(List<EnhancedOrder> orders, List<String> targetStates) {
    List<EnhancedOrder> targetOrders = new ArrayList<EnhancedOrder>();
    nextOrder: for (EnhancedOrder order : orders) {
      if (order.isSetError())
        continue;

      for (OrderLine line : order.getOrderLines()) {
        for (RequestSchedule rs : line.getRequestSchedules()) {
          for (DeliverySchedule ds : rs.getDeliverySchedules()) {
            if (targetStates.contains(ds.getState())) {
              targetOrders.add(order);
              continue nextOrder;
            }
          }
        }
      }
    }
    return targetOrders;
  }

  /**
   * If site calendar is available, add business days as per daysInMilliSec to dateToAddTo
   * else return null;
   * @param rs Request Schedule
   * @param itemId order line item id
   * @param dateToAddTo  buyer approval date
   * @param daysInMilliSec tolerance
   * @param ctx
   * @return valid business day
   */
  public static Calendar addSiteBusinessDaysToDate(
    RequestSchedule rs,
    Calendar dateToAddTo,
    int daysToAdd,
    PlatformUserContext ctx) {
    if (Objects.nonNull(rs) && !FieldUtil.isNull(dateToAddTo)) {
      Calendar startTime = GregorianCalendar.from(Instant.now().minus(daysToAdd * 3, java.time.temporal.ChronoUnit.DAYS) // TODO: Not sure if this logic holds up
        .atZone(java.time.ZoneId.systemDefault()));
      Calendar endTime = GregorianCalendar.from(Instant.now().plus(daysToAdd * 3, java.time.temporal.ChronoUnit.DAYS) // TODO: Not sure if this logic holds up
        .atZone(java.time.ZoneId.systemDefault()));
      Long shipToSiteId = rs.getSysShipToSiteId();
      try {
        BusinessCalendar bc = BusinessCalendarUtil.getSiteCalendar(
          IntrinsicRecurringDataType.WORKING_CALENDAR,
          shipToSiteId,
          startTime,
          endTime,
          (DvceContext) ctx);
        return bc.addDays(dateToAddTo, daysToAdd);
      }
      catch (BusinessCalendarException ex) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Encountered an error getting business calendar. Will continue by just adding flat milliseconds");
          // Fall through to base-case
        }
      }
    }
    // Otherwise - Just add days
    long resultMillis = dateToAddTo.getTimeInMillis() + DvceConstants.DAY * daysToAdd;
    Calendar result = (Calendar) dateToAddTo.clone();
    result.setTimeInMillis(resultMillis);
    return result;
  }

  /**
   * If site calendar is available, add business days as per daysInMilliSec to dateToAddTo
   * @param dateToAddTo 
   * @param leadTime in days
   * @param shipFromSiteId
   * @param ctx
   * @return valid business day
   */
  public static Calendar addSiteBusinessDaysToDate(
    Timestamp dateToAddTo,
    int leadTime,
    Long shipFromSiteId,
    PlatformUserContext ctx) {
    SiteRow siteRow = SiteCacheManager.getInstance().getRow(shipFromSiteId, (DvceContext) ctx);
    Calendar inputDate = Calendar.getInstance();
    inputDate.setTimeInMillis(dateToAddTo.getTime());
    Calendar startTime = GregorianCalendar.from(Instant.now().minus(leadTime * 3, java.time.temporal.ChronoUnit.DAYS) // TODO: Not sure if this logic holds up
      .atZone(TimeZone.getTimeZone(siteRow.getTimeZoneId()).toZoneId()));
    Calendar endTime = GregorianCalendar.from(Instant.now().plus(leadTime * 3, java.time.temporal.ChronoUnit.DAYS) // TODO: Not sure if this logic holds up
      .atZone(TimeZone.getTimeZone(siteRow.getTimeZoneId()).toZoneId()));
    try {
      BusinessCalendar bc = BusinessCalendarUtil.getSiteCalendar(
        IntrinsicRecurringDataType.WORKING_CALENDAR,
        shipFromSiteId,
        startTime,
        endTime,
        (DvceContext) ctx);
      return bc.addDays(inputDate, -leadTime);
    }
    catch (BusinessCalendarException ex) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Encountered an error getting business calendar. Will continue by just adding flat milliseconds");
        // Fall through to base-case
      }
    }
    // Otherwise - Just add days
    long resultMillis = inputDate.getTimeInMillis() - DvceConstants.DAY * leadTime;
    Calendar result = (Calendar) dateToAddTo.clone();
    result.setTimeInMillis(resultMillis);
    return result;
  }

  /**
   * If site calendar is available, add business hours as per leadTime to dateToAddTo
   * @param dateToAddTo 
   * @param leadTime in hours(milliseconds)
   * @param shipFromSiteId
   * @param ctx
   * @return valid business date time
   */
  public static Calendar addSiteBusinessHoursToDate(
    Timestamp dateToAddTo,
    Long leadTime,
    Long shipFromSiteId,
    PlatformUserContext ctx) {
    SiteRow siteRow = SiteCacheManager.getInstance().getRow(shipFromSiteId, (DvceContext) ctx);
    Calendar inputDate = GregorianCalendar.from(
      Instant.now().atZone(TimeZone.getTimeZone(siteRow.getTimeZoneId()).toZoneId()));
    inputDate.setTimeInMillis(dateToAddTo.getTime());
    /* Calendar startTime = GregorianCalendar.from(  Instant.now().minusMillis(leadTime * 5)
      .atZone(TimeZone.getTimeZone(siteRow.getTimeZoneId()).toZoneId()));
    Calendar endTime = GregorianCalendar.from(Instant.now().plusMillis(leadTime * 5)
        .atZone((TimeZone.getTimeZone(siteRow.getTimeZoneId()).toZoneId())));*/
    ZoneId shipFromZone=TimeZone.getTimeZone(siteRow.getTimeZoneId()).toZoneId();
    Instant now=Instant.now().atZone(shipFromZone).toInstant();
    Instant dateToAdd=dateToAddTo.toInstant().atZone(shipFromZone).toInstant();
    Calendar startTime = GregorianCalendar.from(now.minus(182,java.time.temporal.ChronoUnit.DAYS).atZone(shipFromZone));//182 days back to cover half year
    Calendar endTime = GregorianCalendar.from(dateToAdd.plus(182,java.time.temporal.ChronoUnit.DAYS).atZone(shipFromZone)); //182 days ahead to cover half year

    try {
      BusinessCalendar bc = BusinessCalendarUtil.getSiteCalendar(IntrinsicRecurringDataType.WORKING_CALENDAR, shipFromSiteId, startTime, endTime, (DvceContext)ctx);
      Long time =bc.datePlusWorkingTime(inputDate.getTimeInMillis(),-leadTime, TimeZone.getTimeZone(siteRow.getTimeZoneId()), new Date(startTime.getTimeInMillis()));
      Calendar addedTime = Calendar.getInstance(TimeZone.getTimeZone(siteRow.getTimeZoneId()));
      addedTime.setTimeInMillis(time);
      return addedTime;
    }
    catch(BusinessCalendarException ex) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Encountered an error getting business calendar. Will continue by just adding flat milliseconds");
        // Fall through to base-case
      }
    }
    // Otherwise - Just add hours
    long resultMillis = inputDate.getTimeInMillis() - leadTime;
    Calendar result = (Calendar) dateToAddTo.clone();
    result.setTimeInMillis(resultMillis);
    return result;
  }

  /**
   * Populates Promise Unit price from Promise Item.
   *
   * @param ds
   * @param ctx
   */
  public static void populatePromiseUnitPrice(DeliverySchedule ds, PlatformUserContext ctx) {
    if (FieldUtil.isNull(ds.getSysPromiseItemId())
      || (ds.isSetPromiseUnitPriceAmount() && !FieldUtil.isNull(ds.getPromiseUnitPriceAmount()))) {
      return;
    }
    RequestSchedule rs = ds.getParent();
    OrderLine ol = rs.getParent();
    EnhancedOrder order = ol.getParent();
    double price = OrderUtil.getUnitPrice(
      ds.getSysPromiseItemId(),
      order.getSysBuyingOrgId(),
      order.getSysSellingOrgId(),
      rs.getSysShipToSiteId(),
      order.getSysOwningOrgId(),
      null,
      null,
      ctx);

    ds.setPromiseUnitPriceAmount(price);
    ds.setPromiseUnitPriceUOM(ds.getRequestUnitPriceUOM());
    populatePromisePricePer(ds, ctx);
  }

  /**
   * Populates Promise Per from Promise Item.
   *
   * @param ds
   * @param ctx
   */
  public static void populatePromisePricePer(DeliverySchedule ds, PlatformUserContext ctx) {
    DeliveryScheduleMDFs omsDsMdfs = ds.getMDFs(DeliveryScheduleMDFs.class);
    if (FieldUtil.isNull(ds.getSysPromiseItemId())
      || (omsDsMdfs.isSetPromisePricePer() && !FieldUtil.isNull(omsDsMdfs.getPromisePricePer()))) {
      return;
    }
    RequestSchedule rs = ds.getParent();
    OrderLine ol = rs.getParent();
    EnhancedOrder order = ol.getParent();
    double pricePer = OrderUtil.getPricePer(
      ds.getSysPromiseItemId(),
      order.getSysBuyingOrgId(),
      order.getSysSellingOrgId(),
      rs.getSysShipToSiteId(),
      order.getSysOwningOrgId(),
      null,
      null,
      ctx);
    if(!FieldUtil.isNull(pricePer) && pricePer > 0) {
      omsDsMdfs.setPromisePricePer(pricePer);
    }
  }
  /**
   * Populate line details based on Promise Item.
   * If Promise Item is Buyer Item, populate it as is.
   * If Promise Item is Supplier Item, populate the mapped Item. If there is no mapped Item, populate the Ext Item as Supplier Item (converting the AVL PO to Spot PO). 
   *
   *
   * @param ds
   * @param ctx
   */
  public static void populateCategoryLineItemDetails(DeliverySchedule ds, PlatformUserContext ctx) {
    RequestSchedule rs = ds.getParent();
    OrderLine ol = rs.getParent();
    EnhancedOrder order = ol.getParent();

    if (FieldUtil.isNull(ds.getSysId())
      || ((!FieldUtil.isNull(ol.getSysItemId()) || !FieldUtil.isNull(ol.getExtItemName()))
        && (FieldUtil.isNull(ds.getSysPromiseItemId()) && FieldUtil.isNull(ds.getDsExtVendorItemName())))) {
      return;
    }

    if (!FieldUtil.isNull(ds.getDsExtVendorItemName()) && FieldUtil.isNull(ol.getSysItemId())
      && FieldUtil.isNull(ol.getExtItemName())) {
      ol.setExtItemName(ds.getDsExtVendorItemName());
      order.setIsSpot(true);
      return;
    }
    if (FieldUtil.isNull(ds.getSysPromiseItemId())) {
      return;
    }

    boolean isSupplierItem = isSupplierItem(ds.getSysPromiseItemId(), order.getSysBuyingOrgId(), ctx);

    if (FieldUtil.isNull(ol.getSysItemId()) && isSupplierItem) {
      Item buyerItem = getMappedItem(ds.getSysPromiseItemId(), order.getSysOwningOrgId(), ctx);
      if (Objects.nonNull(buyerItem)) {
        ol.setItemEnterpriseName(buyerItem.getEnterpriseName());
        ol.setItemName(buyerItem.getName());
        ol.setSysItemId(buyerItem.getSysId(), false);
      }
      else {
        Item supplierItem = TransactionCache.getItem(ds.getSysPromiseItemId(), ctx);
        ol.setExtItemName(supplierItem.getName());
        //Converting AVL PO to Spot PO.
        order.setIsSpot(true);
      }
    }
    else if (FieldUtil.isNull(ol.getSysItemId())) {
      Item buyerItem = TransactionCache.getItem(ds.getSysPromiseItemId(), ctx);
      ol.setItemEnterpriseName(buyerItem.getEnterpriseName());
      ol.setItemName(buyerItem.getName());
      ol.setSysItemId(buyerItem.getSysId(), false);

      if (!ds.isSetRequestUnitPriceAmount() || FieldUtil.isNull(ds.getRequestUnitPriceAmount())) {
        double price = OrderUtil.getUnitPrice(
          ol.getSysItemId(),
          order.getSysBuyingOrgId(),
          order.getSysSellingOrgId(),
          rs.getSysShipToSiteId(),
          order.getSysOwningOrgId(),
          null,
          ol.getSysProductGroupLevelId(),
          ctx);

        ds.setRequestUnitPriceAmount(price);
        ds.setRequestUnitPriceUOM(order.getCurrency());
      }
    }
  }

  /**
   * Get Mapped Item
   *
   * @param sysPromiseItemId
   * @param sysOwningOrgId
   * @param ctx
   */
  public static Item getMappedItem(Long sysPromiseItemId, Long sysOwningOrgId, PlatformUserContext ctx) {
    ModelRetrieval modelRetrieval = ModelQuery.retrieve(ItemMapping.class);
    modelRetrieval.setIncludeAttachments(ItemMapping.class, false);
    List<ItemMapping> itemMappings = DirectModelAccess.read(
      ItemMapping.class,
      ctx,
      new SqlParams().setLongValue("SYS_MAPPED_ITEM_ID", sysPromiseItemId),
      ModelQuery.sqlFilter("SYS_MAPPED_ITEM_ID = $SYS_MAPPED_ITEM_ID$ and active = 1"),modelRetrieval);
    String buyerEnt = OrganizationCacheManager.getInstance().getOrganization(sysOwningOrgId).getEntName();
    Long itemId = itemMappings.stream().filter(im -> buyerEnt.equals(im.getItemEnterpriseName())).findAny().map(
      ItemMapping::getSysItemId).orElse(null);
    return TransactionCache.getItem(itemId, ctx);
  }
  
  /**
   * Get Mapped Item
   *
   * @param sysPromiseItemId
   * @param sysOwningOrgId
   * @param ctx
   */
  public static  List<Long> getMappedItems(List<Long> sysPromiseItemIds, Long sysOwningOrgId, PlatformUserContext ctx) {
    ModelRetrieval modelRetrieval = ModelQuery.retrieve(ItemMapping.class);
    modelRetrieval.setIncludeAttachments(ItemMapping.class, false);
    List<ItemMapping> itemMappings = DirectModelAccess.read(
      ItemMapping.class,
      ctx,
      new SqlParams().setCollectionValue("SYS_MAPPED_ITEM_ID", sysPromiseItemIds),
      ModelQuery.sqlFilter("SYS_MAPPED_ITEM_ID in $SYS_MAPPED_ITEM_ID$ and active = 1"), modelRetrieval);
    String ents= OrganizationCacheManager.getInstance().getOrganization(sysOwningOrgId).getEntName();
    
   
    List<Long> items = itemMappings.stream().filter(im -> ents.equalsIgnoreCase(im.getItemEnterpriseName())).map(i->i.getSysItemId()).collect(Collectors.toList());
    return items;
  }

  /**
   * Check if Item is Non-buyer item
   *
   * @param sysPromiseItemId
   * @param sysBuyingOrgId
   * @return
   */
  public static boolean isSupplierItem(Long sysPromiseItemId, Long sysBuyingOrgId, PlatformUserContext ctx) {

    Item item = TransactionCache.getItem(sysPromiseItemId, ctx);
    Long itemEntId = item.getSysEnterpriseId();

    Long buyerEntId = OrganizationCacheManager.getInstance().getOrganization(sysBuyingOrgId).getSysEntId();
    return FieldUtil.isDifferent(itemEntId, buyerEntId);
  }

  public static boolean isMappedItem(
    Long sysPromiseItemId,
    Long sysItemId,
    Long sysOwningOrgId,
    PlatformUserContext ctx) {
    Item buyerItem = getMappedItem(sysPromiseItemId, sysOwningOrgId, ctx);
    boolean isMappedItem = buyerItem == null ? false : buyerItem.equals(TransactionCache.getItem(sysItemId, ctx));
    return isMappedItem;
  }

  /**
   * Get Contract From Rs.
   *
   * @param rs
   * @param ctx
   * @return
   */
  public static ContractResult getContractFromRS(RequestSchedule rs, PlatformUserContext ctx) {
    return TransactionCache.getContractFromRS(rs, ctx);
  }

  public static List<ItemSubstitution> getSubstituteItemRows(
		  List<Long> sysItemIds,
		  PlatformUserContext context) {
	  SqlParams params = new SqlParams();
	  params.setCollectionValue("BASE_ITEM_ID", sysItemIds);
	  params.setBooleanValue("IS_ACTIVE", true);
	  List<ItemSubstitution> substituteItemsList = DirectModelAccess.read(
			  ItemSubstitution.class,
			  context,
			  params,
			  ModelQuery.sqlFilter("ITEM_SUBSTITUTION.BASE_ITEM_ID IN $BASE_ITEM_ID$"),
			  ModelQuery.sqlFilter("ITEM_SUBSTITUTION.IS_ACTIVE = $IS_ACTIVE$"),
			  ModelQuery.sqlFilter("ITEM_SUBSTITUTION.RECORD_TYPE = 'Substitute'"),
			  ModelQuery.sqlFilter("ITEM_SUBSTITUTION.START_DATE <= systimestamp"),
			  ModelQuery.sqlFilter("ITEM_SUBSTITUTION.END_DATE >= systimestamp"));
	  return substituteItemsList;
  }

  /**
   * Get Substitute Items
   *
   * @param inputOrder
   */
  public static Map<Long, List<ItemSubstitution>> getSubstituteItems(
    EnhancedOrder inputOrder,
    PlatformUserContext context) {
    List<Long> sysItemIds = inputOrder.getOrderLines().stream().filter(
      line -> !FieldUtil.isNull(line.getSysItemId())).map(OrderLine::getSysItemId).collect(Collectors.toList());

    SqlParams params = new SqlParams();
    params.setCollectionValue("BASE_ITEM_ID", sysItemIds);
    params.setBooleanValue("IS_ACTIVE", true);
    List<ItemSubstitution> substituteItemsList = DirectModelAccess.read(
      ItemSubstitution.class,
      context,
      params,
      ModelQuery.sqlFilter("ITEM_SUBSTITUTION.BASE_ITEM_ID IN $BASE_ITEM_ID$"),
      ModelQuery.sqlFilter("ITEM_SUBSTITUTION.IS_ACTIVE = $IS_ACTIVE$"),
      ModelQuery.sqlFilter("ITEM_SUBSTITUTION.RECORD_TYPE = 'Substitute'"),
      ModelQuery.sqlFilter("ITEM_SUBSTITUTION.START_DATE <= systimestamp"),
      ModelQuery.sqlFilter("ITEM_SUBSTITUTION.END_DATE >= systimestamp"));

    Map<Long, List<ItemSubstitution>> substituteItemsMap = new HashMap<Long, List<ItemSubstitution>>();
    for (ItemSubstitution sItem : substituteItemsList) {
      if (Objects.isNull(substituteItemsMap.get(sItem.getSysBaseItemId()))) {
        List<ItemSubstitution> list = new ArrayList<ItemSubstitution>();
        list.add(sItem);
        substituteItemsMap.put(sItem.getSysBaseItemId(), list);
      }
      else {
        substituteItemsMap.get(sItem.getSysBaseItemId()).add(sItem);
      }
    }

    return substituteItemsMap;
  }

  /**
   * 
   * Set ProductGroupLevel (PGL) hierarchy description 
   * for OrderLines with LineType = Category
   * 
   * PGL is set on OrderLine str_pgl_udf <UDF>
   *
   * @param EnhancedOrder
   * @param DvceContext
   */
  public static void setOrderLinePGLDescUDF(EnhancedOrder order, DvceContext dvceContext) {
    for (OrderLine line : order.getOrderLines()) {
      Long sysPglId = line.getSysProductGroupLevelId();

      if (sysPglId != null) {

        // Handle case in which IssueComputation is requested by time-based Workflow.
        if (dvceContext.getRoleEnterpriseId() == null) {
          UserContext userCtx = new UserContext();
          String buyingOrgEntName = order.getBuyingOrgEnterpriseName();
          EnterpriseKey entKey = new EnterpriseKey(dvceContext.getValueChainId(), buyingOrgEntName);
          EnterpriseRow entRow = EnterpriseCacheManager.getInstance().getEnterprise(entKey, dvceContext);
          Long sysBuyingOrgEntId = entRow.getSysEntId();

          userCtx.setEntName(order.getBuyingOrgEnterpriseName());
          userCtx.setEntId(sysBuyingOrgEntId);
          userCtx.setValueChainId(order.getValueChainId());
          dvceContext = new DvceContext(userCtx);
        }

        String pglHierarchyDescription = ContractRestUtil.getPGLHierarchyById(sysPglId, dvceContext);
        if (pglHierarchyDescription == null)
          pglHierarchyDescription = "";
        line.setUdf("str_pgl_udf", pglHierarchyDescription); //PGL name  
      }
      else {
        line.setUdf("str_pgl_udf", ""); //PGL name  
      }
    }
  }

  /**
   * Deprecate - Use ConvertUomUtil.convertQuantityUOM API instead
   */
  @Deprecated
  public static Optional<Double> convertToPalletsOpt(
    double quantity,
    QuantityUOM uom,
    ItemRow itemRow,
    Buffer buffer,
    PlatformUserContext context) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "convertToPalletsOpt")) {
      Item item = null;
      if (itemRow == null) {
        return Optional.empty();
      }
      else {
        item = JAXBUtil.FACTORY.createItem();
        JaxbSpyUtil.populateJaxbFromDaoRow(item, itemRow, CpConstants.STANDARD_ITEM_TYPE, (DvceContext) context);
      }

      if (buffer == null) {
        LOG.debug("Buffer missing. Falling back to item for conversion to pallets.");
      }
      Optional<Double> pallets = ConvertUomUtil.convertQuantityOpt(item, buffer, quantity, uom, QuantityUOM.PALLET);
      return pallets.map(Math::ceil);
    }
  }

  /**
   * Deprecated - User ConvertUomUtil.convertQuantityUOM API instead
   */
  @Deprecated
  public static Double convertToPallets(
    Double quantity,
    QuantityUOM uom,
    ItemRow itemRow,
    Buffer buffer,
    PlatformUserContext context) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "convertToPallets")) {
      return convertToPalletsOpt(quantity, uom, itemRow, buffer, context).orElse(null);
    }
  }

  public static void populatePromiseItem(DeliverySchedule ds) {
    OrderLine line = ds.getParent().getParent();
    ds.setPromiseItemEnterpriseName(line.getItemEnterpriseName());
    ds.setPromiseItemName(line.getItemName());
    ds.setSysPromiseItemId(line.getSysItemId(), true);
    ds.setAgreedItemName(line.getItemName());
    ds.setSysAgreedItemId(line.getSysItemId(), true);
    new OrderDomainValidator().populateIsHazardousByPromiseItem(ds);
    ds.setDsExtVendorItemName(line.getExtItemName());
  }

  public static void validateEditableFieldsForFullFillmentStates(EnhancedOrder currentOrder, EnhancedOrder inputOrder) {
    //OMO not editable
    if (inFullFillmentStates.contains(currentOrder.getState())
      && ((FieldUtil.isNull(currentOrder.getOrderMgmtOrgName()) && !FieldUtil.isNull(inputOrder.getOrderMgmtOrgName()))
        || (!FieldUtil.isNull(currentOrder.getOrderMgmtOrgName()) && FieldUtil.isNull(inputOrder.getOrderMgmtOrgName()))
        || (!FieldUtil.isNull(currentOrder.getOrderMgmtOrgName()) && !FieldUtil.isNull(inputOrder.getOrderMgmtOrgName())
          && !currentOrder.getOrderMgmtOrgName().equals(inputOrder.getOrderMgmtOrgName())))) {
      inputOrder.setError(
        "OMS.enhancedOrder.updateIntg.fieldCannotBeEdited",
        "OrderMgmtOrgName",
        currentOrder.getState());
    }
  }

  /**
   * @return the get transaction display number
   */
  public static String getTransactionDisplay(Model model) {
    StringJoiner joiner = new StringJoiner("/");
    if (model instanceof EnhancedOrder) {
      if (!FieldUtil.isNull(model.getSysId()))
        joiner.add(((EnhancedOrder) model).getOrderNumber());
      else
        joiner.add("Temp-" + ((EnhancedOrder) model).getOrderNumber());
    }
    else if (model instanceof OrderLine) {
      if (!FieldUtil.isNull(model.getSysId()))
        joiner.add(((OrderLine) model).getParent().getOrderNumber());
      else
        joiner.add("Temp-" + ((OrderLine) model).getParent().getOrderNumber());
      joiner.add(((OrderLine) model).getLineNumber());
    }
    else if (model instanceof RequestSchedule) {
      if (!FieldUtil.isNull(model.getSysId()))
        joiner.add(((RequestSchedule) model).getParent().getParent().getOrderNumber());
      else
        joiner.add("Temp-" + ((RequestSchedule) model).getParent().getParent().getOrderNumber());
      joiner.add(((RequestSchedule) model).getParent().getLineNumber());
      joiner.add(((RequestSchedule) model).getRequestScheduleNumber());
    }
    else if (model instanceof DeliverySchedule) {
      if (!FieldUtil.isNull(model.getSysId()))
        joiner.add(((DeliverySchedule) model).getParent().getParent().getParent().getOrderNumber());
      else
        joiner.add("Temp-" + ((DeliverySchedule) model).getParent().getParent().getParent().getOrderNumber());
      joiner.add(((DeliverySchedule) model).getParent().getParent().getLineNumber());
      joiner.add(((DeliverySchedule) model).getParent().getRequestScheduleNumber());
      joiner.add(((DeliverySchedule) model).getDeliveryScheduleNumber());
    }
    else if (model instanceof Requisition) {
      joiner.add(((Requisition) model).getRequisitionNumber());
    }
    else if (model instanceof RequisitionLine) {
      if (!FieldUtil.isNull(model.getSysId()))
        joiner.add(((RequisitionLine) model).getParent().getRequisitionNumber());
      else
        joiner.add("Temp-" + ((RequisitionLine) model).getParent().getRequisitionNumber());
      joiner.add(((RequisitionLine) model).getLineNumber());
    }
    return joiner.toString();
  }
  
  @SuppressWarnings("unchecked")
  public static List<EnhancedOrder> getCachedCurrent(ActionBasedWorkflowContext<EnhancedOrder> context,Boolean isIgnoreCache) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getCachedCurrent")) {
      if(!isIgnoreCache) {
        return  getCachedCurrent(context);
      }
      List<Long> orderIds = context.getCurrent().getModels().stream().map(EnhancedOrder::getSysId).collect(
        Collectors.toList());
      if(!orderIds.isEmpty()) {
       return new ArrayList<>(
                DirectModelAccess.readByIds(EnhancedOrder.class, orderIds, context.getPlatformUserContext()).values());
      } else {
        return Collections.emptyList();
      }
      
    }
  }

  @SuppressWarnings("unchecked")
  public static List<EnhancedOrder> getCachedCurrent(ActionBasedWorkflowContext<EnhancedOrder> context) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getCachedCurrent")) {
      WorkflowCache wfCache = ((ActionBasedWorkflowContextImpl) context).getWfCache();
      List<EnhancedOrder> cached = (List<EnhancedOrder>) wfCache.getAttribute("CACHED_CURRENT_FROM_DB");
      if (cached != null)
        return cached;
      List<Long> orderIds = context.getCurrent().getModels().stream().map(EnhancedOrder::getSysId).collect(
        Collectors.toList());
      if(!orderIds.isEmpty()) {
        List<EnhancedOrder> orders =  new ArrayList<>(
          DirectModelAccess.readByIds(EnhancedOrder.class, orderIds, context.getPlatformUserContext()).values());
        wfCache.setAttribute("CACHED_CURRENT_FROM_DB", orders);
        return orders;
      } else {
        return Collections.emptyList();
      }

    }
  }

  /**
   * 
   * Remove all DSs without a specified state from all orders
   *
   * @param ordersToFilter
   * @param withoutState
   * @return listOfFilteredOrders
   */
  public static List<EnhancedOrder> filterOutOrderLinesWithoutState(
    List<EnhancedOrder> ordersToFilter,
    String withoutState) {

    for (Iterator<EnhancedOrder> orderIter = ordersToFilter.iterator(); orderIter.hasNext();) {
      EnhancedOrder order = orderIter.next();

      for (Iterator<OrderLine> olineIter = order.getOrderLines().iterator(); olineIter.hasNext();) {
        OrderLine oline = olineIter.next();

        for (Iterator<RequestSchedule> rsIter = oline.getRequestSchedules().iterator(); rsIter.hasNext();) {
          RequestSchedule rs = rsIter.next();

          for (Iterator<DeliverySchedule> dsIter = rs.getDeliverySchedules().iterator(); dsIter.hasNext();) {
            DeliverySchedule ds = dsIter.next();
            if (!ds.getState().equals(withoutState)) {
              dsIter.remove();
            }
          }

          if (rs.getDeliverySchedules().size() == 0)
            rsIter.remove();
        }
        if (oline.getRequestSchedules().size() == 0)
          olineIter.remove();
      }
      if (order.getOrderLines().size() == 0)
        orderIter.remove();
    }

    return ordersToFilter;
  }
  
  
  public static EnhancedOrder filterOutOrderLinesWithoutDS(
		    EnhancedOrder order,
		    List<Long> withoutIds) {

		    

		      for (Iterator<OrderLine> olineIter = order.getOrderLines().iterator(); olineIter.hasNext();) {
		        OrderLine oline = olineIter.next();

		        for (Iterator<RequestSchedule> rsIter = oline.getRequestSchedules().iterator(); rsIter.hasNext();) {
		          RequestSchedule rs = rsIter.next();

		          for (Iterator<DeliverySchedule> dsIter = rs.getDeliverySchedules().iterator(); dsIter.hasNext();) {
		            DeliverySchedule ds = dsIter.next();
		            if (!withoutIds.contains(ds.getSysId())) {
		              dsIter.remove();
		            }
		          }

		          if (rs.getDeliverySchedules().size() == 0)
		            rsIter.remove();
		        }
		        if (oline.getRequestSchedules().size() == 0)
		          olineIter.remove();
		      }
		      if (order.getOrderLines().size() == 0)
		        return null;
		    

		    return order;
		  }

  /**
   * 
   * Filter out lines without a specified state from an order
   *
   * @param orderToFilter
   * @param withoutState
   * @return filteredOrder or null (all lines filtered out)
   */
  public static EnhancedOrder filterOutOrderLinesWithoutState(EnhancedOrder orderToFilter, String withoutState) {
    List<EnhancedOrder> filteredOrders = filterOutOrderLinesWithoutState(
      Collections.singletonList(orderToFilter),
      withoutState);
    if (filteredOrders.size() > 0)
      return filteredOrders.get(0);
    else
      return null;
  }

  public static Pair getPackedItemQuantityAndUOM(String packageTypeName, long itemId, long orgId, DvceContext ctx) {
    PackingResource packingResource = getPackingResourceFromItem(packageTypeName, orgId, ctx);
    if (Objects.nonNull(packingResource)) {
      List<CompatibleItem> compactibleItemList = packingResource.getCompatibleItems().stream().filter(
        item -> item.getResourceModelLevel().equals(Item.MODEL_LEVEL_TYPE.toString())
          && item.getResourceId() == itemId).sorted(Comparator.comparingLong(c -> c.getSysId())).collect(
            Collectors.toList());
      if (!Objects.isNull(compactibleItemList) && !compactibleItemList.isEmpty()) {
        CompatibleItem compactibleItem = compactibleItemList.get(0);
        ItemRow item = ItemCacheManager.getInstance().getItem(compactibleItem.getResourceId());
        String orderingUOM = compactibleItem.getStockingUOM();
        if (Objects.nonNull(item)) {
          ItemKey key = new ItemKey(item.getVcId(), item.getEntName(), packageTypeName);
          ItemRow packingItem = ItemCacheManager.getInstance().getItem(key);
          if (Objects.nonNull(packingItem) && (!FieldUtil.isNull(packingItem.getOrderingUom()) || !FieldUtil.isNull(packingItem.getOrderUom())))
            orderingUOM = getOrderingUOM(packingItem, null, null);
        }
        return new Pair<>(compactibleItem.getQuantity(), orderingUOM);
      }
    }
    else {
      if (!FieldUtil.isNull(orgId)) {
        OrganizationRow orgRow = OrganizationCacheManager.getInstance().getOrganization(orgId);
        if (Objects.nonNull(orgRow)) {
          ItemKey key = new ItemKey(ctx.getValueChainId(), orgRow.getEntName(), packageTypeName);
          ItemRow packingItem = ItemCacheManager.getInstance().getItem(key);
          if (Objects.nonNull(packingItem)) {
        	  String uom =null;
            if (Objects.nonNull(packingItem)) {
            	uom = getOrderingUOM(packingItem, null, null);
            }
            if(!FieldUtil.isNull(uom))
              return new Pair<>(
                0,
                uom);
          }
        }
      }
    }
    return null;
  }

  public static PackingResource getPackingResourceFromItem(String packageTypeName, long orgId, DvceContext ctx) {
    Map<String, Object> filters = new HashMap<String, Object>();
    filters.put("SCC_PACKING_RESOURCE.SYS_ORG_ID", orgId);
    filters.put("SCC_PACKING_RESOURCE.NAME", packageTypeName);
    List<PackingResource> packingResource = ModelDataServiceUtil.readModelByAttributes(
      PackingResource.class,
      filters,
      Services.get(UserContextService.class).createDefaultValueChainAdminContext(ctx.getValueChainId()));
    if (Objects.nonNull(packingResource) && !packingResource.isEmpty()) {
      return packingResource.get(0);
    }
    return null;
  }

  public static List<EnhancedOrder> findSysIdOfEnhancedOrder(List<EnhancedOrder> orders, PlatformUserContext ctx) {
    SqlParams sqlParams = new SqlParams();
    List<EnhancedOrder> dBOrders = new ArrayList<EnhancedOrder>();
    for (EnhancedOrder order : orders) {
      String order_number = order.getOrderNumber();
      sqlParams.setStringValue("ORDER_NUMBER", order_number);
      sqlParams.setLongValue("VC_ID", ctx.getValueChainId());
      String sqlFilter = "SCC_ENHANCED_ORDER.VC_ID = $VC_ID$ and SCC_ENHANCED_ORDER.ORDER_NUMBER = $ORDER_NUMBER$";
      List<EnhancedOrder> enhancedOrdercList = ModelDataServiceUtil.readModelByAttributesAndParam(
        EnhancedOrder.class,
        sqlFilter,
        sqlParams,
        ctx);
      if (enhancedOrdercList.size() > 0) {
        dBOrders.add(enhancedOrdercList.get(0));
      }
    }
    return dBOrders;
  }

  public static String getOrderType(Model model) {
    String orderType = "";
    if (model instanceof EnhancedOrder) {
      orderType = OrderRestUtil.getOrderType((EnhancedOrder) model);
    }
    else if (model instanceof OrderLine) {
      orderType = OrderRestUtil.getOrderType(((OrderLine) model).getParent());
    }
    else if (model instanceof RequestSchedule) {
      orderType = OrderRestUtil.getOrderType(((RequestSchedule) model).getParent().getParent());
    }
    else if (model instanceof DeliverySchedule) {
      orderType = OrderRestUtil.getOrderType(((DeliverySchedule) model).getParent().getParent().getParent());
    }
    return orderType;
  }

  /**
   * Looks up Site-Hierarchy polices for the order based on the Ship-To-Site.
   * Handles cases where site is not present (Ship To Address instead of site) by falling back to owning org
   */
  public static <T> T getOrderSitePolicy(EnhancedOrder order, Policy<T> policy, PlatformUserContext context) {
    if (policy == null) {
      throw new IllegalArgumentException("Cannot look up null policy");
    }
    Long siteId = order.getFirstRequestSchedule().getSysShipToSiteId();
    if (isSingleShipToSite(order) && !FieldUtil.isNull(siteId)) {
      return TransactionCache.getSitePolicy(policy.getName(), siteId, policy.getDefaultValue(), context);
    }
    else {
      return TransactionCache.getOrgPolicy(
        policy.getName(),
        order.getSysOwningOrgId(),
        policy.getDefaultValue(),
        context);
    }
  }
  
  /**
   * Looks up Site-Hierarchy polices for the order based on the Ship-To-Site.
   * Handles cases where site is not present (Ship To Address instead of site) by falling back to owning org
   */
  public static <T> T getOrderShipFromSitePolicy(EnhancedOrder order, Policy<T> policy, PlatformUserContext context) {
    if (policy == null) {
      throw new IllegalArgumentException("Cannot look up null policy");
    }
    if (isSingleShipFromSite(order)) {
	  Long siteId =null;
      for (OrderLine line : order.getOrderLines()) {
        for (RequestSchedule rs : line.getRequestSchedules()) {
          for (DeliverySchedule ds : rs.getDeliverySchedules()) {
            if (!FieldUtil.isNull(ds.getSysShipFromSiteId())) {
              siteId = ds.getSysShipFromSiteId();
              break;
            }
          }
        }
      } 
      if(Objects.nonNull(siteId)) {
        return TransactionCache.getSitePolicy(policy.getName(), siteId, policy.getDefaultValue(), context);
      } else {
        return TransactionCache.getOrgPolicy(
          policy.getName(),
          order.getSysOwningOrgId(),
          policy.getDefaultValue(),
          context);
      }
    }
    else {
      return TransactionCache.getOrgPolicy(
        policy.getName(),
        order.getSysOwningOrgId(),
        policy.getDefaultValue(),
        context);
    }
  }

  public static <T> T getDeploymentOrderSitePolicyFromShipment(
    Shipment shipment,
    Policy<T> policy,
    PlatformUserContext context) {
    Long siteId = shipment.getSysShiptoSiteId();
    Long orgId = shipment.getSysBuyOrgId();
    if (!FieldUtil.isNull(siteId)) {
      return TransactionCache.getSitePolicy(policy.getName(), siteId, policy.getDefaultValue(), context);
    }
    return TransactionCache.getOrgPolicy(policy.getName(), orgId, policy.getDefaultValue(), context);
  }

  private static boolean isSingleShipToSite(EnhancedOrder order) {
    final String TRANS_KEY = "isSingleShipToSite";
    if (Objects.nonNull(order.getTransientField(TRANS_KEY)) 
      && !StringUtils.isNullOrBlank(order.getTransientField(TRANS_KEY))) {
      return (Boolean) order.getTransientField(TRANS_KEY);
    }

    Long site = null;
    for (OrderLine line : order.getOrderLines()) {
      for (RequestSchedule rs : line.getRequestSchedules()) {
        Long rsSite = rs.getSysShipToSiteId();
        if (site == null || FieldUtil.isNull(site)) {
          site = rsSite;
        }
        else if (!FieldUtil.isNull(rsSite) && !site.equals(rsSite)) {
          order.setTransientField(TRANS_KEY, false);
          return false;
        }
      }
    }
    boolean result = (site != null); // If there's no site, it's not a single site
    order.setTransientField(TRANS_KEY, result);
    return result;
  }

  private static boolean isSingleShipFromSite(EnhancedOrder order) {
    Long site = null;
    for (OrderLine line : order.getOrderLines()) {
      for (RequestSchedule rs : line.getRequestSchedules()) {
        for (DeliverySchedule ds : rs.getDeliverySchedules()) {
          if (!FieldUtil.isNull(ds.getSysShipFromSiteId())) {
            Long dsSite = ds.getSysShipFromSiteId();
            if (site == null || FieldUtil.isNull(site)) {
              site = dsSite;
            }
            else if (!FieldUtil.isNull(dsSite) && !site.equals(dsSite)) {
              return false;
            }
          }
        }
      }
    }
    boolean result = (site != null); // If there's no site, it's not a single site
    return result;
  }

  public static boolean isAdditionalCostEditable(EnhancedOrder order, PlatformUserContext context) {
    if(context.isDerivedFrom("VALUE_CHAIN_ADMIN")) { //Allow VC Admin to change costs for anything - Doing this for allowing syncing Shipment cost on to order.
      return true;
    }
    Long userOrg = context.getRoleOrganizationId();
    List<Long> editingOrgs = Stream.of(order.getSysOwningOrgId(), order.getSysOrderMgmtOrgId())
      .filter(Objects::nonNull)
      .collect(toList());
    if(!editingOrgs.contains(userOrg)) {
      return false;
    }
    List<String> acceptableStates = Arrays.asList(
      "Draft", //Why is this not in the constants?
      States.AWAITING_APPROVAL,
      States.NEW,
      States.BUYER_CHANGE_REQUESTED,
      States.BUYER_CONFIRMED_WITH_CHANGES,
      States.VENDOR_CHANGE_REQUESTED,
      States.VENDOR_CONFIRMED_WITH_CHANGES,
      States.OPEN);
    if(acceptableStates.contains(order.getState())) {
      return true;
    }
    List<String> fulfillmentStateGroupStates = Arrays.asList(
      States.IN_FULFILLMENT,
      States.IN_TRANSIT,
      States.PARTIALLY_RECEIVED,
      States.PARTIALLY_SHIPPED,
      States.RECEIVED);
    if(!fulfillmentStateGroupStates.contains(order.getState())) {
      return false;
  }
    Policy<Boolean> policy = getAdditionalCostPolicy(order.getOrderType());
    if(policy != null) {
      boolean allowInFulfillment = TransactionCache.getOrgPolicy(policy, order.getSysOwningOrgId(), context);
      return allowInFulfillment;
    } else {
      return false;
    }
  }

  private static Policy<Boolean> getAdditionalCostPolicy(
    String orderType) {
    OrderTypeEnum enumValue = OrderTypeEnum.get(orderType);
    switch(enumValue) {
      case PURCHASE_ORDER:
        return Policy.ALLOW_ADDITIONAL_COST_IN_FULFILLMENT_STATE_GROUP_PO;
      case SALES_ORDER:
        return Policy.ALLOW_ADDITIONAL_COST_IN_FULFILLMENT_STATE_GROUP_SO;
      case DEPLOYMENT_ORDER:
        return Policy.ALLOW_ADDITIONAL_COST_IN_FULFILLMENT_STATE_GROUP_DO;
      case RETURN_ORDER:
        return Policy.ALLOW_ADDITIONAL_COST_IN_FULFILLMENT_STATE_GROUP_RO;
    }
    LOG.error("No Additional Cost In Fulfillment Policy for Order Type (" + orderType + ") - Defaulting to false.");
    return null;
  }

  /**
   *conversion Request and Promise Unit price to Uesr profile currency format
   * @return 
   * 
   */
  public static double convertPriceToUserProfileCurrencyFormat(Double unitPriceAmount, PlatformUserContext ctx) {
	  if(unitPriceAmount==null)
		  return 0.0;
	  try {
		  PlatformUserProfile userProfile = Services.get(UserContextService.class).getPlatformUserProfile(ctx);
		  DecimalFormat twoDForm = new DecimalFormat(userProfile.getCurrencyFormat());
		  return Double.parseDouble(twoDForm.format(unitPriceAmount).replaceAll(",", ""));
	  }catch(Exception e) {
		  NumberFormat numFormat = NumberFormat.getInstance(Locale.getDefault());
		  return Double.parseDouble(numFormat.format(unitPriceAmount));
	  }
  }
  
  public static String convertQtyToProfileNumberFormat(Double number, PlatformUserContext ctx) {
    if(number==null)
      return "0";
    try {
    	PlatformUserProfile userProfile = Services.get(UserContextService.class).getPlatformUserProfile(ctx);
    	DecimalFormat twoDForm = new DecimalFormat(userProfile.getNumberFormat());
    	return twoDForm.format(number).replaceAll(",", "");
    }catch(Exception e) {
    	NumberFormat numFormat = NumberFormat.getInstance(Locale.getDefault());
    	return numFormat.format(number);
    }
  }

  /**
   * Sets the order line number based on the policy "OMS.OrderLineNumberFormat".
   * Includes the *hidden* feature to include the Order Number.
   */
  public static void setLineNumber(OrderLine line, int number, PlatformUserContext context) {
	  Boolean shouldKeepUILineNumbering = false;
    EnhancedOrder eo = line.getParent();
	  if(line.getLineNumber() != null && (line.getLineNumber().indexOf(".") != -1 
	    || line.getLineNumber().indexOf("TempLine-") != -1) 
	    && (eo.getOrigin() != null && eo.getOrigin().equals(OriginEnum.UI.stringValue())) ) {
		  shouldKeepUILineNumbering = true;
	  } else if(eo.getOrigin() != null && eo.getOrigin().equals(OriginEnum.UI.stringValue()) ) {
		  for(OrderLine ol: eo.getOrderLines()) {
			  if(SCCEnhancedOrderConstants.States.CONVERTED.equals(ol.getState())) {
				  // For split kits, most likely the first OL will be in Converted <state>
				  shouldKeepUILineNumbering = true;
				  break;
			  }
		  }
	  }
	  
	  String formatted = "";
	  if(shouldKeepUILineNumbering) {
	      String lineNumber = line.getLineNumber();
	      String policyLineFormat = TransactionCache.getOrgPolicy(Policy.ORDER_LINE_NUMBER_FORMAT, line.getParent().getSysOwningOrgId(), context);
	      String policyLineNumSeparator = TransactionCache.getOrgPolicy(Policy.LINE_NUMBER_SEPARATOR, line.getParent().getSysOwningOrgId(), context);
	      
	      lineNumber = lineNumber.replace("TempLine-","");
        StringBuilder lineNumberResult = new StringBuilder(12);
        if(lineNumber.split("\\.").length > 1) {
            String[] lineNumberParts = lineNumber.split("\\.");
            int partsCounter = 0;
            for(String lineNumberPart: lineNumberParts) {
                Integer lineNum = Integer.valueOf(lineNumberPart);
                lineNumberResult.append(String.format(policyLineFormat,lineNum));
                if(lineNumberResult.length() > 0 && partsCounter < lineNumberParts.length-1)  {
                  lineNumberResult.append(policyLineNumSeparator);
                }
                partsCounter++;
            }
        } else {
            String formattedLineNumber = String.format(policyLineFormat,Integer.valueOf(lineNumber));
            lineNumberResult.append(formattedLineNumber);
        }
        formatted = lineNumberResult.toString();
	  } else {
    		formatted = getFormattedLineNumber(number, line.getParent().getSysOwningOrgId(), context);
    		//TODO: At the time of writing this (22.0) I am unsure if the feature regarding auto-generating order line #s
    		//From integ in the form OL + OrderNumber is used. This is here to give a path to allow continuing to generate
    		//line numbers in this manner. This feature is not intended to be announced. If we have no queries about how
    		//to get the old behavior back by the time we reach 24.0, this feature should be removed then.
    		formatted = formatted.replaceAll("\\$ORDER_NUMBER\\$", line.getParent().getOrderNumber());
	  }
	  
	  line.setLineNumber(formatted);  
  }

  /**
   * Gives the line number as it should be formatted by the policy "OMS.OrderLineNumberFormat".
   * Note: Does not include the *hidden* feature of including the Order Number. That must be handled
   * by the caller.
   */
  public static String getFormattedLineNumber(int number, long orgId, PlatformUserContext context) {
    String format = TransactionCache.getOrgPolicy(Policy.ORDER_LINE_NUMBER_FORMAT, orgId, context);
    return String.format(format, number);
  }

  public static boolean shouldGenerateLineNumber(OrderLine line,boolean acceptUploadedOrderNo) {
    String lineNumber = line.getLineNumber();
    boolean acceptGivenLineNumber = acceptUploadedOrderNo || OrderUtil.hasAutoGeneratedLineNumber(line);
    return !acceptGivenLineNumber || FieldUtil.isNull(lineNumber) || lineNumber.startsWith(GENERATE_LINE_NUMBER);
  }

  public static boolean shouldGenerateOrderNumber(
    EnhancedOrder inputEO,
    boolean isAutoGeneratedOrderNumber,
    PlatformUserContext ctx) {
    boolean shouldGenerateOrderNumber = false;
    if (FieldUtil.isNull(inputEO.getOrderNumber()) || isAutoGeneratedOrderNumber)
      return true;
    boolean isFromIXM = !FieldUtil.isNull(inputEO.getOrigin()) && inputEO.getOrigin().contains("IXM") ? true : false;
    if (isFromIXM)
      return shouldGenerateOrderNumber;
    boolean isFromInteg = isFromIntegration(inputEO);
    if (isFromInteg) {
      boolean acceptUploadedOrderNo = OrgProcPolicyUtil.getBoolean(
        OrgProcPolicyUtil.getPolicy(
          inputEO,
          OrgProcPolicyConstants.ACCEPT_UPLOADED_ORDER_NUMBER,
          OrgProcPolicyConstants.YES,
          (DvceContext) ctx));
      if (!acceptUploadedOrderNo && FieldUtil.isNull(inputEO.getOrderNumber())) {
        shouldGenerateOrderNumber = true;
      }

    }
    else {
      shouldGenerateOrderNumber = true;
    }
    return shouldGenerateOrderNumber;
  }

  public static boolean hasAutoGeneratedLineNumber(OrderLine line) {
    boolean lineExistsInDb = line.getSysId() != null;
    if (lineExistsInDb) {
      return true; // Treat an existing line as auto-generated because the system has already had the chance to make one.
    }
    Object value = line.getTransientField("isAutoGeneratedLineNumber");
    if (value == null) {
      return false;
    }
    return Boolean.TRUE.equals(value);
  }

  /**
   * Remove buyer or seller attachments
   * Attachment WritePermissions: enterprise <dimension>
   *
   * @param context
   */
  public static void removeBuyerOrSellerAttachments(ActionBasedWorkflowContext<EnhancedOrder> context) {
    for (EnhancedOrder order : context.getInput().getModels()) {
      EnhancedOrderMDFs orderMDFs = order.getMDFs(EnhancedOrderMDFs.class);
      if (!context.getPlatformUserContext().isDerivedFrom(SCCPrivateConstants.RoleTypes.ORCHESTRATOR)) {
        Pair<Pair<Boolean, Boolean>, Pair<Boolean, Boolean>> contextPair = OrderRestUtil.determineOrderContext(
          order,
          context.getPlatformUserContext(),
          null,
          "",false,OrderRestUtil.getEnterpriseRoleType((DvceContext)context.getPlatformUserContext()));
        Boolean isBuyerContext = contextPair.first.first;
        if (isBuyerContext) {
          orderMDFs.getSellerAttachments().clear();
        }
        else {
          orderMDFs.getBuyerAttachments().clear();
        }
      }
    }
  }

  public static String replaceIsDerivedFromMacro(String sqlDef, PlatformUserContext userContext) {
    SqlParams params = new SqlParams();
    SqlDefHelper.setDefaultParams(sqlDef, params, (DvceContext) userContext);
    DerivedFromRoleMacro macro = new DerivedFromRoleMacro(params);
    Matcher matcher = Pattern.compile("\\$\\{isDerivedFromRole:.*\\}").matcher(sqlDef);
    StringBuffer result = new StringBuffer();
    while (matcher.find()) {
      String match = matcher.group();
      String insides = match.substring("${isDerivedFromRole:".length(), match.length() - 1);
      List<String> fragments = Arrays.asList(insides.split(","));
      String replacement = macro.process(fragments);
      matcher.appendReplacement(result, replacement);
    }
    matcher.appendTail(result);
    return result.toString();
  }
  
  public static Boolean isDisableParentSyncEnabled(Long entId,PlatformUserContext context) {
    return TransactionCache.getEntPolicy(OMSConstants.Policies.DISABLE_PARENT_ORDER_RESYNC, entId,false,context);
   }

  public static String replaceAliases(String sqlDef) {
    final String prefixes = "([,\\s=\\(])";
    sqlDef = sqlDef.replaceAll(prefixes + "eo\\.", "$1scc_enhanced_order\\.");
    String hierarchyPrefix = "\\$\\{MyAndChildOrgIds:";
    sqlDef = sqlDef.replaceAll(hierarchyPrefix + "eo\\.", "\\$\\{MyAndChildOrgIds:scc_enhanced_order\\.");
    return sqlDef;
  }

  public static void validateProgramAndPrgmEnt(CsvRow row, DvceContext context) throws CsvTransformException {
    String programEnterpriseName = row.getAllColumnNames().contains("ProgramEnterpriseName") ? row.get("ProgramEnterpriseName") 
      : DvceConstants.NULL_STRING_VALUE;
    String programName = row.getAllColumnNames().contains("ProgramName") ? row.get("ProgramName") : DvceConstants.NULL_STRING_VALUE ;
    if (!FieldUtil.isNull(programEnterpriseName) || !FieldUtil.isNull(programName)) {
      ModelDataService modelDataService = Services.get(ModelDataService.class);
      EnterpriseRow ent = EnterpriseCacheManager.getInstance().getRow(
        new EnterpriseKey(context.getValueChainId(), programEnterpriseName),
        context);
      if (ent != null) {
        SqlParams params = new SqlParams();
        params.setStringValue("NAME", programName);
        params.setLongValue("ENTERPRISE_ID", ent.getSysEntId());
        List<Program> programList = modelDataService.read(
          Program.class,
          context,
          params,
          ModelQuery.sqlFilter("SCC_PROGRAM.NAME = $NAME$ AND SYS_ENTERPRISE_ID=$ENTERPRISE_ID$ "));
        if (programList.size() == 0) {
          throw new CsvTransformException(
            "OMS.enhancedOrder.Integ.UnableToLookUpForProgram",
            new Object[] { programName });
        }
      }
      else {
        throw new CsvTransformException(
          "OMS.enhancedOrder.Integ.InvalidProgramNameAndEnterpriseName",
          new Object[] { programName, programEnterpriseName });
      }
    }
  }

  /**
   * This method is to create Order Tracking Event of Type Line Added.
   * This event get generated after line added to order.
   *
   * @param context
   */
  public static void generateOrderTrackingEventForChildLevelAddedType(ActionBasedWorkflowContext<EnhancedOrder> context) {

    List<EnhancedOrder> cachedCurrentOrders = OrderUtil.getCachedCurrent(context);
    List<EnhancedOrder> currentOrders = context.getCurrent().getModels();

    if (!cachedCurrentOrders.isEmpty() && context.getCurrent().getErrors().isEmpty()) {
      for (EnhancedOrder cachedCurrentOrder : cachedCurrentOrders) {
        List<EnhancedOrder> ordersToCreateOrderTrackingEvent = new ArrayList<EnhancedOrder>();
        EnhancedOrder currentOrder = ModelUtil.findMatching(cachedCurrentOrder, currentOrders);
        List<String> eventTypes = new ArrayList<>();
        if (null != currentOrder) {
          eventTypes = getEventTypesToBeGenerated(currentOrder, cachedCurrentOrder);
          if(Objects.nonNull(eventTypes)) {
            ordersToCreateOrderTrackingEvent.add(currentOrder);  
          }
        }
        if (!ordersToCreateOrderTrackingEvent.isEmpty() && (Objects.nonNull(eventTypes) && !eventTypes.isEmpty())) {
          for(String eventType : eventTypes) {
            OrderTrackingUtil.addActionDrivenTrackingEvent(
              ordersToCreateOrderTrackingEvent,
              context.getPlatformUserContext(),
              eventType);
          }
        }
      }
    }
  }
  
  private static List<String> getEventTypesToBeGenerated(EnhancedOrder currentOrder, EnhancedOrder cachedOrder) {
    List<String> eventTypes = new ArrayList<>();
    if(currentOrder.getOrderLines().size() > cachedOrder.getOrderLines().size()) {
      eventTypes.add(OrderTrackingEventTypeEnum.LINE_ADDED.toString());
    }
    getEventTypeRSLevel(currentOrder, cachedOrder, eventTypes);
    return eventTypes;
  }

  /**
   * @param currentOrder
   * @param cachedOrder
   * @param eventTypes
   */
  private static void getEventTypeRSLevel(
    EnhancedOrder currentOrder,
    EnhancedOrder cachedOrder,
    List<String> eventTypes) {
      for (OrderLine cachedLine : cachedOrder.getOrderLines()) {
        OrderLine currentLine = ModelUtil.findMatching(cachedLine, currentOrder.getOrderLines());
        if (null != currentLine && currentLine.getRequestSchedules().size() > cachedLine.getRequestSchedules().size()) {
          eventTypes.add(OrderTrackingEventTypeEnum.REQUEST_SCHEDULE_ADDED.toString());
        }
        getEventTypeDSLevel(eventTypes, cachedLine, currentLine);
    }
  }

  /**
   * @param eventTypes
   * @param cachedLine
   * @param currentLine
   */
  private static void getEventTypeDSLevel(List<String> eventTypes, OrderLine cachedLine, OrderLine currentLine) {
          for (RequestSchedule cachedRS : cachedLine.getRequestSchedules()) {
            RequestSchedule currentRS = ModelUtil.findMatching(cachedRS, currentLine.getRequestSchedules());
            if (null != currentRS && currentRS.getDeliverySchedules().size() > cachedRS.getDeliverySchedules().size()) {
              eventTypes.add(OrderTrackingEventTypeEnum.DELIVERY_SCHEDULE_ADDED.toString());
            }
          }
        }
  
  public static boolean isStandardOrderBrokerageEnabled(Long entId) {
	  Set<Feature> subscriptions = FeatureSubscriptionCacheManager.getInstance().getSubscriptions(entId);
	  boolean disabled = false;
	  if (Objects.isNull(subscriptions)) {
		  return false;
	  }

	  for (Feature subscription: subscriptions) {
		  if (subscription.getName().equals(OMSConstants.Features.STANDARD_ORDER_BROKERAGE)) {
			  return true;
		  }
	  }
	  return disabled;
  }
  
  public static Set<EnhancedOrder> eligibleToUpdateParentOrder(List<EnhancedOrder> orders,boolean isStandard) {
	  Set<EnhancedOrder> eligibleOrders = new HashSet<>();
	  for(EnhancedOrder order : orders) {
		  Long owningOrg = OrderTypeEnum.PURCHASE_ORDER.toString().equals(order.getOrderType()) ? order.getSysSellingOrgId() : 
			  order.getSysOwningOrgId();
		  OrganizationRow orgRow = OrganizationCacheManager.getInstance().getOrganization(owningOrg);
		  if(Objects.nonNull(orgRow)) {
			  boolean standardBrokerage = OrderUtil.isStandardOrderBrokerageEnabled(orgRow.getSysEntId());
			  if(isStandard) {
				  if(standardBrokerage) {
					  eligibleOrders.add(order);
				  }
			  }else {
				  if(!standardBrokerage) {
					  eligibleOrders.add(order);
				  }
			  }
		  }
	  }
	  return eligibleOrders;
  }
  
  public static boolean eligibleToCreateFulfillmentOrder(Long owningOrg) {
	  OrganizationRow orgRow = OrganizationCacheManager.getInstance().getOrganization(owningOrg);
	  if(Objects.nonNull(orgRow)) {
		  boolean standardBrokerage = OrderUtil.isStandardOrderBrokerageEnabled(orgRow.getSysEntId());
		  if(standardBrokerage) {
			  return true;
		  }
	  }
	  return false;
  }

  public static boolean compareOrders(EnhancedOrder order1, EnhancedOrder order2) {
    return (order1 == null && order2 == null) || (order1 == null && order2 != null)
      || (order1 != null && order2 == null)
      || ((order1.getValueChainId() == (order2.getValueChainId()))
        && (((FieldUtil.isNull(order1.getOwningOrgName())) && (FieldUtil.isNull(order2.getOwningOrgName())))
          || (!FieldUtil.isDifferent(order1.getOwningOrgName(), order2.getOwningOrgName())))
        && (((FieldUtil.isNull(order1.getOwningOrgEnterpriseName()))
          && (FieldUtil.isNull(order2.getOwningOrgEnterpriseName())))
          || (!FieldUtil.isDifferent(order1.getOwningOrgEnterpriseName(), order2.getOwningOrgEnterpriseName())))
        && (((FieldUtil.isNull(order1.getAuxiliaryKey())) && (FieldUtil.isNull(order2.getAuxiliaryKey())))
          || (!FieldUtil.isDifferent(order1.getAuxiliaryKey(), order2.getAuxiliaryKey())))
        && ((FieldUtil.isNull(order1.getOrderNumber()) && FieldUtil.isNull(order2.getOrderNumber()))
          || (!FieldUtil.isDifferent(order1.getOrderNumber(), order2.getOrderNumber()))
          || (!FieldUtil.isDifferent(
            order1.getMDFs(EnhancedOrderMDFs.class).getTempOrderNumber(),
            order2.getOrderNumber()))));
  }

  public static Hold getHold(Hold hold, PlatformUserContext platformUserContext) {
    SqlParams sqlParams = new SqlParams();
    sqlParams.setLongValue("TRANSACTION_ID", hold.getTransactionId());
    sqlParams.setStringValue("TRANSACTION_MODEL_LEVEL", hold.getTransactionModelLevel());
    sqlParams.setLongValue("SYS_HOLD_REASON_CODE_ID", hold.getSysHoldReasonCodeId());
    sqlParams.setStringValue("HOLD_TYPE", hold.getHoldReasonCodeHoldType());
    String sql = "TRANSACTION_ID = $TRANSACTION_ID$ AND TRANSACTION_MODEL_LEVEL = $TRANSACTION_MODEL_LEVEL$ AND SYS_HOLD_REASON_CODE_ID = $SYS_HOLD_REASON_CODE_ID$ "
      + "AND HOLD_TYPE = $HOLD_TYPE$";
    List<Hold> dbHold = DirectModelAccess.read(Hold.class, platformUserContext, sqlParams, ModelQuery.sqlFilter(sql));
    if (Objects.nonNull(dbHold) && !dbHold.isEmpty()) {
      return dbHold.get(0);
    }
    return null;
  }

  public static void delinkRequisitionIfRequired(List<EnhancedOrder> orders) {
    for (EnhancedOrder order : orders) {
      boolean clearOrderFlag = true;
      for (OrderLine line : order.getOrderLines()) {
        if (!FieldUtil.isNull(line.getSysRequisitionLineId()) && !ignoreStates.contains(line.getState())
          && (States.VENDOR_REJECTED.equals(line.getState()))) {
          line.setTransientField("reqLineId", line.getSysRequisitionLineId());
          line.setSysRequisitionLineId(DvceConstants.NULL_LONG_VALUE, true);
        }
        else if (!FieldUtil.isNull(line.getSysRequisitionLineId()) && !ignoreStates.contains(line.getState())) {
          clearOrderFlag = false;
        }
        else {
          line.setTransientField("deleteOrCancel", "deleteOrCancel");
        }
      }

      if (clearOrderFlag) {
        order.setTransientField("reqId", order.getSysRequisitionId());
        order.setSysRequisitionId(DvceConstants.NULL_LONG_VALUE, true);
      }
    }
  }

  public static boolean getIsAutoReceipt(EnhancedOrder order, PlatformUserContext ctx) {
    if (!order.isIsSpot()) {
      for (OrderLine line : order.getOrderLines()) {
        for (RequestSchedule rs : line.getRequestSchedules()) {
          AvlLine avlLine = OrderUtil.getAVLFromRS(rs, ctx);
          if (null != avlLine) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                "AVL Line: [ Partner =" + avlLine.getPartnerName() + " Item Name =" + avlLine.getItemName()
                  + " Site Name =" + avlLine.getSiteName() + "]");
            }
            if (avlLine.isAutoReceipt()) {
              return true;
            }
          }
        }
      }
    }
    PartnerRow partnerRow = PartnerUtil.getPartner(order.getSysVendorId(), ctx);
    if (partnerRow != null && partnerRow.getIsActive() == 1) {
      return partnerRow.isOmsAutoReceipt();
    }
    return false;
  }

  public static boolean getIsAutoMoveToReceived(EnhancedOrder order, PlatformUserContext ctx) {
    if (!order.isIsSpot()) {
      for (OrderLine line : order.getOrderLines()) {
        for (RequestSchedule rs : line.getRequestSchedules()) {
          AvlLine avlLine = OrderUtil.getAVLFromRS(rs, ctx);
          if (null != avlLine) {
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                "AVL Line: [ Partner =" + avlLine.getPartnerName() + " Item Name =" + avlLine.getItemName()
                  + " Site Name =" + avlLine.getSiteName() + "]");
            }
            AvlLineMDFs avlLineMDFs = avlLine.getMDFs(AvlLineMDFs.class);
            if (avlLineMDFs.isAutoMoveToReceived()) {
              return true;
            }
          }
        }
      }
    }
    PartnerRow partnerRow = PartnerUtil.getPartner(order.getSysVendorId(), ctx);
    if (partnerRow != null && partnerRow.getIsActive() == 1) {
      return partnerRow.isOmsAutoMoveToReceived();
    }
    return false;
  }

  /**
   * TODO this method will check if requisition number present for order. If present then checks if requisition line number is provided or not.
   *
   * @param row
   * @throws CsvTransformException 
   */
  public static void validateIfOrderFromRequisition(CsvRow row, DvceContext dvceContext) throws CsvTransformException {
    String requisitionNumber = row.get("RequisitionNumber");
    String requisitionLineLineNumber = row.get("RequisitionLineLineNumber");
    String requisitionRequestingOrgEnterpriseName = row.get("RequisitionRequestingOrgEnterpriseName");
    String requisitionRequestingOrgName = row.get("RequisitionRequestingOrgName");
    List<String> eiligibleRequisitionStates = new ArrayList<String>();
    eiligibleRequisitionStates.add(com.ordermgmtsystem.supplychaincore.mpt.SCCRequisitionConstants.States.APPROVED);
    eiligibleRequisitionStates.add(
      com.ordermgmtsystem.supplychaincore.mpt.SCCRequisitionConstants.States.PARTIALLY_APPROVED);
    eiligibleRequisitionStates.add(
      com.ordermgmtsystem.supplychaincore.mpt.SCCRequisitionConstants.States.PARTIALLY_CONVERTED);
    if (!FieldUtil.isNull(requisitionNumber)) {
      if (FieldUtil.isNull(requisitionLineLineNumber)) {
        throw new CsvTransformException(
          OrderLine.MODEL_LEVEL_TYPE,
          "OMS.enhancedOrder.transformer.requisitionLineNotProvided",
          new Object[] { requisitionNumber });
      }
      else {
        ModelDataService mds = Services.get(ModelDataService.class);
        SqlParams param = new SqlParams();
        param.setValue("REQUISITION_NUMBER", requisitionNumber);
        param.setValue("LINE_NUMBER", requisitionLineLineNumber);
        List<Requisition> modelList = mds.read(
          Requisition.class,
          dvceContext,
          param,
          ModelQuery.sqlFilter("SCC_REQUISITION.REQUISITION_NUMBER = $REQUISITION_NUMBER$"),
          ModelQuery.sqlFilter("SCC_REQUISITION_LINE.LINE_NUMBER = $LINE_NUMBER$"));
        if (modelList != null && !modelList.isEmpty()) {
          if (eiligibleRequisitionStates.contains(modelList.get(0).getState())) {
            row.set("RequisitionLineRequestingOrgEnterpriseName", requisitionRequestingOrgEnterpriseName);
            row.set("RequisitionLineRequestingOrgName", requisitionRequestingOrgName);
            row.set("LineRequisitionNumber", requisitionNumber);
          }
          else
            throw new CsvTransformException(
              OrderLine.MODEL_LEVEL_TYPE,
              "OMS.enhancedOrder.transformer.requisitionStateInvalid",
              new Object[] { requisitionNumber });
        }
        else
          throw new CsvTransformException(
            OrderLine.MODEL_LEVEL_TYPE,
            "OMS.enhancedOrder.transformer.requisitionNotFound",
            new Object[] { requisitionLineLineNumber, requisitionNumber });
      }
    }
  }

  /**
   * This method will validate and populate missing values for DeliveryCommodityCode.
   *
   * @param row
   * @throws CsvTransformException 
   */
  
  public static void validateDeliveryCommodityCodes(CsvRow row) throws CsvTransformException{
    
    if (!FieldUtil.isNull(row.get("DeliveryCommodityCodeLevel1Name"))) {

        String level1_name = row.get("DeliveryCommodityCodeLevel1Name");
        String level2_name = row.get("DeliveryCommodityCodeLevel2Name");
        String level3_name = row.get("DeliveryCommodityCodeLevel3Name");
        String level4_name = row.get("DeliveryCommodityCodeLevel4Name");
        String level5_name = row.get("DeliveryCommodityCodeLevel5Name");
        String dCCPGEntName = row.get("DeliveryCommodityCodeProductGroupEnterpriseName");
        String dCCPGTypeName = row.get("DeliveryCommodityCodeProductGroupTypeName");
        
        SqlParams params = new SqlParams();
        StringBuilder sql = new StringBuilder(
          "select 1 from product_group_level where ENT_NAME=$ENT_NAME$ AND PGRP_TYPE_NAME = $PGRP_TYPE_NAME$ AND (IS_ACTIVE IS NULL OR IS_ACTIVE = 1)");

        
        if(!StringUtil.isNullOrBlank(row.get("DeliveryCommodityCodeLevel1Name"))) {
          sql.append("and LEVEL1_NAME=$LEVEL1_NAME$");
          params.setStringValue("LEVEL1_NAME", level1_name);
        }
        
        if(!StringUtil.isNullOrBlank(row.get("DeliveryCommodityCodeLevel2Name"))) {
          sql.append("and LEVEL2_NAME=$LEVEL2_NAME$");
          params.setStringValue("LEVEL2_NAME", level2_name);
        }
        
        if(!StringUtil.isNullOrBlank(row.get("DeliveryCommodityCodeLevel3Name"))) {
          sql.append("and LEVEL3_NAME=$LEVEL3_NAME$");
          params.setStringValue("LEVEL3_NAME", level3_name);
        }
        
        if(!StringUtil.isNullOrBlank(row.get("DeliveryCommodityCodeLevel4Name"))) {
          sql.append("and LEVEL4_NAME=$LEVEL4_NAME$");
          params.setStringValue("LEVEL4_NAME", level4_name);
        }
        
        if(!StringUtil.isNullOrBlank(row.get("DeliveryCommodityCodeLevel5Name"))) {
          sql.append("and LEVEL5_NAME=$LEVEL5_NAME$");
          params.setStringValue("LEVEL5_NAME", level5_name);
        }
        
        params.setStringValue("ENT_NAME", dCCPGEntName);
        params.setStringValue("PGRP_TYPE_NAME", "Commodity Code Hierarchy");
        
        
        SqlService sqlService = Services.get(SqlService.class);
        SqlResult result = sqlService.executeQuery(sql.toString(), params);
        if (result != null && result.getRows().size() == 0) {
          throw new CsvTransformException(
            Order.MODEL_LEVEL_TYPE,
            "Unable to lookup DeliveryCommodityCode for the provided DeliveryCommodityCodeLevel1Name :" + level1_name);
        }

      if (StringUtil.isNullOrBlank(row.get("DeliveryCommodityCodeLevel2Name")))
        row.set("DeliveryCommodityCodeLevel2Name", "NONE");
      if (StringUtil.isNullOrBlank(row.get("DeliveryCommodityCodeLevel3Name")))
        row.set("DeliveryCommodityCodeLevel3Name", "NONE");
      if (StringUtil.isNullOrBlank(row.get("DeliveryCommodityCodeLevel4Name")))
        row.set("DeliveryCommodityCodeLevel4Name", "NONE");
      if (StringUtil.isNullOrBlank(row.get("DeliveryCommodityCodeLevel5Name")))
        row.set("DeliveryCommodityCodeLevel5Name", "NONE");
      if (StringUtil.isNullOrBlank(row.get("DeliveryCommodityCodeProductGroupEnterpriseName")))
        row.set("DeliveryCommodityCodeProductGroupEnterpriseName", row.get("OwningOrgEnterpriseName"));
      if (StringUtil.isNullOrBlank(row.get("DeliveryCommodityCodeProductGroupTypeName")))
        row.set("DeliveryCommodityCodeProductGroupTypeName", "Commodity Code Hierarchy");
    }
    
  }
  
  
  /**
   * This method will validate and populate missing values for PickupCommodityCode.
   *
   * @param row
   * @throws CsvTransformException 
   */
  
 public static void validatePickupCommodityCodes(CsvRow row) throws CsvTransformException{
    
    if (!FieldUtil.isNull(row.get("PickupCommodityCodeLevel1Name"))) {

        String level1_name = row.get("PickupCommodityCodeLevel1Name");
        String level2_name = row.get("PickupCommodityCodeLevel2Name");
        String level3_name = row.get("PickupCommodityCodeLevel3Name");
        String level4_name = row.get("PickupCommodityCodeLevel4Name");
        String level5_name = row.get("PickupCommodityCodeLevel5Name");
        String pCCPGEntName = row.get("PickupCommodityCodeProductGroupEnterpriseName");
        String pCCPGTypeName = row.get("PickupCommodityCodeProductGroupTypeName");
        
        SqlParams params = new SqlParams();
        StringBuilder sql = new StringBuilder(
          "select 1 from product_group_level where ENT_NAME=$ENT_NAME$ AND PGRP_TYPE_NAME = $PGRP_TYPE_NAME$ AND (IS_ACTIVE IS NULL OR IS_ACTIVE = 1)");

        
        if(!StringUtil.isNullOrBlank(row.get("PickupCommodityCodeLevel1Name"))) {
          sql.append("and LEVEL1_NAME=$LEVEL1_NAME$");
          params.setStringValue("LEVEL1_NAME", level1_name);
        }
        
        if(!StringUtil.isNullOrBlank(row.get("PickupCommodityCodeLevel2Name"))) {
          sql.append("and LEVEL2_NAME=$LEVEL2_NAME$");
          params.setStringValue("LEVEL2_NAME", level2_name);
        }
        
        if(!StringUtil.isNullOrBlank(row.get("PickupCommodityCodeLevel3Name"))) {
          sql.append("and LEVEL3_NAME=$LEVEL3_NAME$");
          params.setStringValue("LEVEL3_NAME", level3_name);
        }
        
        if(!StringUtil.isNullOrBlank(row.get("PickupCommodityCodeLevel4Name"))) {
          sql.append("and LEVEL4_NAME=$LEVEL4_NAME$");
          params.setStringValue("LEVEL4_NAME", level4_name);
        }
        
        if(!StringUtil.isNullOrBlank(row.get("PickupCommodityCodeLevel5Name"))) {
          sql.append("and LEVEL5_NAME=$LEVEL5_NAME$");
          params.setStringValue("LEVEL5_NAME", level5_name);
        }
        
        params.setStringValue("ENT_NAME", pCCPGEntName);
        params.setStringValue("PGRP_TYPE_NAME", "Commodity Code Hierarchy");
        
        
        SqlService sqlService = Services.get(SqlService.class);
        SqlResult result = sqlService.executeQuery(sql.toString(), params);
        if (result != null && result.getRows().size() == 0) {
          throw new CsvTransformException(
            Order.MODEL_LEVEL_TYPE,
            "Unable to lookup PickupCommodityCode for the provided PickupCommodityCodeLevel1Name :" + level1_name);
        }

      if (StringUtil.isNullOrBlank(row.get("PickupCommodityCodeLevel2Name")))
        row.set("PickupCommodityCodeLevel2Name", "NONE");
      if (StringUtil.isNullOrBlank(row.get("PickupCommodityCodeLevel3Name")))
        row.set("PickupCommodityCodeLevel3Name", "NONE");
      if (StringUtil.isNullOrBlank(row.get("PickupCommodityCodeLevel4Name")))
        row.set("PickupCommodityCodeLevel4Name", "NONE");
      if (StringUtil.isNullOrBlank(row.get("PickupCommodityCodeLevel5Name")))
        row.set("PickupCommodityCodeLevel5Name", "NONE");
      if (StringUtil.isNullOrBlank(row.get("PickupCommodityCodeProductGroupEnterpriseName")))
        row.set("PickupCommodityCodeProductGroupEnterpriseName", row.get("SellingOrgEnterpriseName"));
      if (StringUtil.isNullOrBlank(row.get("PickupCommodityCodeProductGroupTypeName")))
        row.set("PickupCommodityCodeProductGroupTypeName", "Commodity Code Hierarchy");
    }
    
  }

  public static void setIncoTermsAndIncoTermsLocation(EnhancedOrder order, Map<RequestSchedule, AvlLine> avlLines, PlatformUserContext ctx) {

    if (!order.isIsSpot() && (OrderTypeEnum.PURCHASE_ORDER.stringValue().equalsIgnoreCase(order.getOrderType()) 
          || OrderTypeEnum.SALES_ORDER.stringValue().equalsIgnoreCase(order.getOrderType())) ) {
      
      if (!order.getOrderLines().isEmpty()) {
        if (!order.getOrderLines().get(0).getRequestSchedules().isEmpty()) {
          if (OrderUtil.isContract(order)) {
            setIncoTermsAndLocationFromContract(order, ctx);
          }
          else {
            setIncoTermsAndLocation(order, avlLines,  ctx);
          }
        }
      }
      
    }
  }

  /**
   * 
   * Set order Inco Terms and Location
   *  Lookup: 
   *  - PO: AVL or Vendor Master if not on AVL
   *  - SO: Customer Master
   *
   * @param order
 * @param avlLines 
   * @param ctx
   */
  private static void setIncoTermsAndLocation(EnhancedOrder order, Map<RequestSchedule, AvlLine> avlLines, PlatformUserContext ctx) {
    
    if( OrderTypeEnum.PURCHASE_ORDER.stringValue().equalsIgnoreCase(order.getOrderType()) ) {
      setIncoTermsAndLocationFromAVL(order, avlLines, ctx);  
    } else if( OrderTypeEnum.SALES_ORDER.stringValue().equalsIgnoreCase(order.getOrderType())  ) {
      setIncoTermsAndLocationFromCustomer(order,ctx);  
    }
    
  }
  
  /**
   * 
   * Get and set Inco Terms and Location on SO
   *
   * @param order
   * @param ctx
   */
  private static void setIncoTermsAndLocationFromCustomer(EnhancedOrder order, PlatformUserContext ctx) {
    PartnerRow partnerRow = PartnerUtil.getPartner(order.getSysCustomerId(), ctx);
    if (partnerRow != null && partnerRow.getIsActive() == 1) {
      if (!FieldUtil.isNull(partnerRow.getOmsIncoTerms()) && FieldUtil.isNull(order.getIncoTerms())) {
        order.setIncoTerms(partnerRow.getOmsIncoTerms());
        order.getOrderLines().stream().forEach(orderLine -> orderLine.setLnIncoTerms(partnerRow.getOmsIncoTerms()));
        
        if (!FieldUtil.isNull(partnerRow.getOmsIncoTermsLocation())
          && FieldUtil.isNull(order.getMDFs(EnhancedOrderMDFs.class).getIncoTermsLocation())) {
          order.getMDFs(EnhancedOrderMDFs.class).setIncoTermsLocation(partnerRow.getOmsIncoTermsLocation());
        }
      }
    }
  }
  
  private static void setIncoTermsAndLocationFromAVL(EnhancedOrder order, Map<RequestSchedule, AvlLine> avlLines, PlatformUserContext ctx) {
    Boolean isGetFromVendor = false;
    String incoTermsLocation = null;
    OrderLine firstLine = null;
    for (OrderLine orderLine : order.getOrderLines()) {
      if (firstLine == null)
        firstLine = orderLine;
      AvlLine avlLine = null;
      if(avlLines != null && avlLines.containsKey(orderLine.getRequestSchedules().get(0))) {
    	  avlLine = avlLines.get(orderLine.getRequestSchedules().get(0));
      } else {
    	  avlLine = OrderUtil.getAVLFromRS(orderLine.getRequestSchedules().get(0), ctx);
      }
      if (null != avlLine) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "AVL Line: [ Partner =" + avlLine.getPartnerName() + " Item Name =" + avlLine.getItemName() + " Site Name ="
              + avlLine.getSiteName() + "]");
        }
        AvlLineMDFs avlLineMDFs = avlLine.getMDFs(AvlLineMDFs.class);
        if( !FieldUtil.isNull(avlLine.getIncoTerms())) { 
          if((!FieldUtil.isNull(order.getOrigin()) && order.getOrigin().equals("UI"))
            || (FieldUtil.isNull(orderLine.getLnIncoTerms()) && (!FieldUtil.isNull(order.getOrigin()) && !order.getOrigin().equals("UI")))) {
            orderLine.setLnIncoTerms(avlLine.getIncoTerms());
          }
          
          if (!FieldUtil.isNull(avlLineMDFs.getIncoTermsLocation()) && firstLine.getLineNumber() == orderLine.getLineNumber()) {
              if( (!FieldUtil.isNull(order.getOrigin()) && order.getOrigin().equals("UI")) || (FieldUtil.isNull(order.getMDFs(EnhancedOrderMDFs.class).getIncoTermsLocation()) && (!FieldUtil.isNull(order.getOrigin()) && !order.getOrigin().equals("UI"))) ) {
                incoTermsLocation = avlLineMDFs.getIncoTermsLocation(); 
              }
          }
            
        } else {
          isGetFromVendor = true;
        }

      }
      else {
        isGetFromVendor = true;
      }
    }
    if (!isGetFromVendor && FieldUtil.isNull(order.getIncoTerms()) ) {
      order.setIncoTerms(order.getOrderLines().get(0).getLnIncoTerms());
      if (incoTermsLocation != null)
        order.getMDFs(EnhancedOrderMDFs.class).setIncoTermsLocation(incoTermsLocation);
    }
    
    if (isGetFromVendor) {
      setIncoTermsAndLocationFromVendor(order, ctx);
    }
  }

  private static void setIncoTermsAndLocationFromVendor(EnhancedOrder order, PlatformUserContext ctx) {
    PartnerRow partnerRow = PartnerUtil.getPartner(order.getSysVendorId(), ctx);
    if (partnerRow != null && partnerRow.getIsActive() == 1) {
      if (!FieldUtil.isNull(partnerRow.getOmsIncoTerms()) && FieldUtil.isNull(order.getIncoTerms())) {
        order.setIncoTerms(partnerRow.getOmsIncoTerms());
        order.getOrderLines().stream().forEach(orderLine -> orderLine.setLnIncoTerms(partnerRow.getOmsIncoTerms()));
        if (!FieldUtil.isNull(partnerRow.getOmsIncoTermsLocation())
          && FieldUtil.isNull(order.getMDFs(EnhancedOrderMDFs.class).getIncoTermsLocation())) {
          order.getMDFs(EnhancedOrderMDFs.class).setIncoTermsLocation(partnerRow.getOmsIncoTermsLocation());
        }
      }
    }
  }
  

  private static void setIncoTermsAndLocationFromContract(EnhancedOrder order, PlatformUserContext ctx) {
    Boolean isGetFromContractLine = false;
    Boolean isGetFromMaster = false;
    String incoTermsLocation = null;
    OrderLine firstLine = null;
    for (OrderLine orderLine : order.getOrderLines()) {
      ContractResult contract = OrderUtil.getContractFromRS(orderLine.getRequestSchedules().get(0), ctx);
      if (firstLine == null)
        firstLine = orderLine;
      if (contract != null) {

        if (!FieldUtil.isNull(contract.getPriceTypeAsInt())
          && (contract.getPriceTypeAsInt() == PricingModelEnum.VOLUME_TIERED.intValue()
            || contract.getPriceTypeAsInt() == PricingModelEnum.TIME_VARYING.intValue()
            || contract.getPriceTypeAsInt() == PricingModelEnum.TIME_VARYING___VOLUME_TIERED.intValue())) {
          if (contract.getContractLine() != null && !FieldUtil.isNull(contract.getContractLine().getLnIncoTerms())) {
            orderLine.setLnIncoTerms(contract.getContractLine().getLnIncoTerms());

            if (!FieldUtil.isNull(contract.getContractLine().getLnIncoTermsLocation())
              && firstLine.getLineNumber() == orderLine.getLineNumber())
              incoTermsLocation = contract.getContractLine().getLnIncoTermsLocation();
          }
          else
            isGetFromContractLine = true;
        }
        else
          isGetFromContractLine = true;
        OmsContractLineRow line = TransactionCache.getContractLineById(contract.getContractLineId(), ctx);

        if (line != null && !FieldUtil.isNull(line.getLnIncoTerms()) && isGetFromContractLine) {
          orderLine.setLnIncoTerms(line.getLnIncoTerms());
          if (!FieldUtil.isNull(line.getLnIncoTermsLocation())
            && firstLine.getLineNumber() == orderLine.getLineNumber()) {
            incoTermsLocation = (line.getLnIncoTermsLocation());
          }
        }
        else if (isGetFromContractLine)
          isGetFromMaster = true;

      }
      else
        isGetFromMaster = true;

      if (isGetFromMaster && OrderTypeEnum.PURCHASE_ORDER.stringValue().equalsIgnoreCase(order.getOrderType())) {
        setIncoTermsAndLocationFromVendor(order, ctx);
      } else if (isGetFromMaster && OrderTypeEnum.SALES_ORDER.stringValue().equalsIgnoreCase(order.getOrderType())) {
        setIncoTermsAndLocationFromCustomer(order, ctx);
      }
      
    }
    List<EnhancedOrder> currentOrders = null;
    EnhancedOrder currentOrder = null;
    List<Long> orderIds= new ArrayList<Long>();
    if(Objects.isNull(currentOrders) && !FieldUtil.isNull(order.getSysId())) {
      orderIds.add(order.getSysId());
        currentOrders = OrderUtil.getOrdersFromIds(orderIds, ctx);
    }
    if (currentOrders != null) {
      for(EnhancedOrder curOrder : currentOrders) {
        if(!FieldUtil.isNull(curOrder.getSysId()) && !FieldUtil.isNull(order.getSysId())
              && curOrder.getSysId().equals(order.getSysId())) {
          currentOrder =curOrder;
        }
      }
      boolean isAvl = OrderUtil.isAVL(order);
      if(currentOrder == null)
      currentOrder = ModelUtil.findMatching(order, currentOrders);
      if(currentOrder != null && isAvl) {
        EnhancedOrderUtil.populateSysIdFromNaturalKey(currentOrder, ctx);
      }
    }
    Boolean differentContract= false;
    Boolean differentTerms = false;
    if(currentOrder!=null) {
      Boolean allowResourcingReleasePO = TransactionCache.getOrgPolicy(Policy.ALLOW_RESOURCING_RELEASE_PO,order.getSysOwningOrgId() , ctx);
      allowResourcingReleasePO = allowResourcingReleasePO!=null ? allowResourcingReleasePO:false;
      EnhancedOrderMDFs orderMDFs = order.getMDFs(EnhancedOrderMDFs.class);
      EnhancedOrderMDFs currentOrderMDFs = currentOrder.getMDFs(EnhancedOrderMDFs.class);
      String currentContractNo = null;
      String currentContractTerm = null;
      if (currentOrderMDFs != null) {
        currentContractNo = currentOrderMDFs.getContractNumber();
        currentContractTerm = currentOrderMDFs.getContractTermsNumber();
      }             
      String inputContractNo = orderMDFs.getContractNumber();
      String inputContractTerm = orderMDFs.getContractTermsNumber();
      if (allowResourcingReleasePO ) {
        if(!FieldUtil.isNull(currentContractNo) && !FieldUtil.isNull(inputContractNo) && !currentContractNo.equalsIgnoreCase(inputContractNo)) {
        differentContract = true;
        }
        if(!FieldUtil.isNull(currentContractTerm) && !FieldUtil.isNull(inputContractTerm) && !currentContractTerm.equalsIgnoreCase(inputContractTerm) ) {
          differentTerms = true;
        }
      }
    }
    if (!isGetFromMaster && (FieldUtil.isNull(order.getIncoTerms())|| differentContract || differentTerms)) {
      order.setIncoTerms(order.getOrderLines().get(0).getLnIncoTerms());
      order.getMDFs(EnhancedOrderMDFs.class).setIncoTermsLocation(incoTermsLocation);
    }
  }

  public static boolean isBlanketRelease(EnhancedOrder order) {
    final String IS_BLANKET_TRANS_KEY = "isBlanketRelease";
    Boolean transValue = (Boolean) order.getTransientField(IS_BLANKET_TRANS_KEY);
    if (transValue != null) {
      return transValue;
    }
    boolean value = false;
    Long contractId = EnhancedOrderMDFs.from(order).getSysContractId();
    if (!FieldUtil.isNull(contractId)) {
      PlatformUserContext vcAdmin = Services.get(UserContextService.class).createDefaultValueChainAdminContext(
        order.getValueChainId());
      ModelRetrieval modelRetrieval = ModelQuery.retrieve(Contract.class);
      modelRetrieval.setIncludeAttachments(Contract.class, false);
      Contract contract = DirectModelAccess.readById(Contract.class, contractId, vcAdmin, modelRetrieval);
      if(Objects.nonNull(contract)) {
        value = ContractTypeEnum.BLANKET.toString().equals(contract.getContractType());  
      }
    }
    order.setTransientField(IS_BLANKET_TRANS_KEY, value);
    return value;
  }


  public static boolean getAsyncAutoShipmentPolicyValue(EnhancedOrder order, PlatformUserContext pltContext) {
    String isAsync=ExternalReferenceUtil.getLocalValue(ShipmentConstants.REF_TYPE_ASYNC_AUTO_ASN, ShipmentConstants.ENABLE_ASYNC_AUTO_ASN);
    if(isAsync!=null )
      return Boolean.valueOf(isAsync); 
     return false;
  }
  
  public static boolean getAsyncAutoShipmentPolicyValue(PlatformUserContext pltContext) {
    String isAsync=ExternalReferenceUtil.getLocalValue(ShipmentConstants.REF_TYPE_ASYNC_AUTO_ASN, ShipmentConstants.ENABLE_ASYNC_AUTO_ASN);
    if(isAsync!=null )
      return Boolean.valueOf(isAsync); 
     return false;
  }
  
  
  public static boolean getAsyncOrderResyncPolicyValue(PlatformUserContext pltContext) {
    String isAsync=ExternalReferenceUtil.getLocalValue(EnhancedOrderConstants.REF_TYPE_ASYNC_ORDER_RESYNC, EnhancedOrderConstants.ENABLE_ASYNC_ORDER_RESYNC);
    if(isAsync!=null )
      return Boolean.valueOf(isAsync); 
     return false;
  }

  public static Map<Boolean, List<EnhancedOrder>> sortOrdersBasedOnAsyncAutoShipmentPolicy(
    List<EnhancedOrder> orders,
    PlatformUserContext context) {

    Map<Boolean, List<EnhancedOrder>> asyncAutoShipmentPolicyOrders = new HashMap<>();
    if (!CollectionUtils.isEmpty(orders)) {
      boolean isAsync = OrderUtil.getAsyncAutoShipmentPolicyValue(context);
      for (EnhancedOrder order : orders) {
        if (asyncAutoShipmentPolicyOrders.get(isAsync) == null) {
          ArrayList<EnhancedOrder> newList = new ArrayList<EnhancedOrder>();
          newList.add(order);
          asyncAutoShipmentPolicyOrders.put(isAsync, newList);
        }
        else {
          asyncAutoShipmentPolicyOrders.get(isAsync).add(order);
        }

      }
    }
    return asyncAutoShipmentPolicyOrders;
  }
  
  /**
   * Inserts grid task pins for {@code taskId} and comma-separated
   * string of objects surrogate ids {@code sysIdString}.
   *
   * @param taskId grid task id.
   * @param sysIdString comma-separate string of object surrogate ids.
   * @param mlt objects {@link com.ordermgmtsystem.platform.data.model.impl.ModelLevelType ModelLevelType}.
   */
  public static void insertGridTaskPin(Long taskId, String sysIdString, ModelLevelType mlt) {
    if (sysIdString == null || taskId == null || mlt == null) {
      LOG.debug(
        "Grid Task Pin creation is skipped. Required parameter is missing " + String.valueOf(taskId) + ", ["
          + sysIdString + "], " + mlt);
      return;
    }
    SqlParams sqlParams = new SqlParams();
    SqlService sqlService = Services.get(SqlService.class);
    sqlParams.setLongValue("SYS_TASK_ID", taskId);
    sqlParams.setStringValue("IDS_AS_STRING", sysIdString);
    sqlParams.setStringValue("MLT", mlt.getValue());
    sqlService.executeQuery("OMS.EnhancedOrderSqls", "InsertGridTaskPin", sqlParams);
  }

  /**
   * Gets indication of existence of pending grid task based on grid task pins.
   *  
   * The result is {@code true} if there is pending (IDLE or RUNNING)
   * grid task, otherwise {@code false}.
   *
   * @param sysIds set of object surrogate ids.
   * @param mlt objects {@link com.ordermgmtsystem.platform.data.model.impl.ModelLevelType ModelLevelType}.
   * @return true if there is pending grid task, false otherwise.
   */
  public static boolean isGridTaskPending(Set<Long> sysIds, ModelLevelType mlt) {
    if (CollectionUtils.isEmpty(sysIds) || mlt == null) {
      LOG.debug(
        "Grid Task Pending lookup is skipped. Required parameter is missing "
          + (sysIds == null ? "null" : sysIds.size()) + " " + mlt);
      return false;
    }
    SqlParams sqlParams = new SqlParams();
    SqlService sqlService = Services.get(SqlService.class);
    sqlParams.setCollectionValue("IDS", sysIds);
    sqlParams.setStringValue("MLT", mlt.getValue());
    sqlParams.setCollectionValue("STATES", ListUtil.create(TaskState.IDLE, TaskState.RUNNING));
    SqlResult sqlResult = sqlService.executeQuery("OMS.EnhancedOrderSqls", "GridTaskByPin", sqlParams);
    if (sqlResult.getRows().isEmpty()) {
      LOG.debug("No grid tasks returned for [" + StringUtils.collectionToDelimitedString(sysIds, ",") + "], " + mlt);
      return false;
    }
    return true;
  }
  
  /**
   * Gets collection of object sysIds those have pending grid tasks
   * association with grid task pins.
   *  
   * The result is a collection of object surrogate ids with IDLE
   * grid tasks of {@code gridTaskType}.
   *
   * @param sysIds set of object surrogate ids.
   * @param mlt objects {@link com.ordermgmtsystem.platform.data.model.impl.ModelLevelType ModelLevelType}.
   * @param gridTaskType grid task type, if {@code null} then parameter is ignored.
   * @return collection of object surrogate ids with IDLE grid tasks of {@code gridTaskType}.
   */
  public static Collection<Long> getIdsWithIdleGridTask(Set<Long> sysIds, ModelLevelType mlt, String gridTaskType) {
    if (CollectionUtils.isEmpty(sysIds) || mlt == null) {
      LOG.debug(
        "Pending Grid Tasks lookup is skipped. Required parameter is missing "
          + (sysIds == null ? "null" : sysIds.size()) + " " + mlt);
      return Collections.emptySet();
    }
    SqlParams sqlParams = new SqlParams();
    SqlService sqlService = Services.get(SqlService.class);
    sqlParams.setCollectionValue("IDS", sysIds);
    sqlParams.setStringValue("MLT", mlt.getValue());
    sqlParams.setCollectionValue("STATES", ListUtil.create(TaskState.IDLE));
    if (gridTaskType == null) {
      sqlParams.setNullStringValue("TASK_TYPE");
    }
    else {
      sqlParams.setStringValue("TASK_TYPE", gridTaskType);
    }
    SqlResult sqlResult = sqlService.executeQuery("OMS.EnhancedOrderSqls", "GetIdsWithGridTasks", sqlParams);
    if (sqlResult.getRows().isEmpty()) {
      LOG.debug("No ids returned for [" + StringUtils.collectionToDelimitedString(sysIds, ",") + "], " + mlt + (gridTaskType != null ? ", " + gridTaskType : ""));
      return Collections.emptySet();
    }
    Collection<Long> ids = new HashSet<>();
    for (SqlRow row : sqlResult.getRows()) {
      ids.add(row.getLongValue("SYS_REF_ID"));
    }
    return ids;
  }
  
  public static Long getServiceLevel(DeliverySchedule deliverySchedule) {
    Long serviceLevel = deliverySchedule.getParent().getSysRsServiceLevelId();
    if (serviceLevel == null) {
      serviceLevel = deliverySchedule.getParent().getParent().getSysLnServiceLevelId();
    }
    if (serviceLevel == null) {
      serviceLevel = deliverySchedule.getParent().getParent().getParent().getSysServiceLevelId();
    }
    return serviceLevel;
  }
  
  /**
   * removes grid task pins related to DSIDs provided
   *  This is required if Async ASN creation is enabled and ASN is created manually fix for PDS-23008
   *
   * @param sysIds set of object surrogate ids.
   * @param mlt objects {@link com.ordermgmtsystem.platform.data.model.impl.ModelLevelType ModelLevelType}.
   */
  public static boolean deleteGridTaskPins(Set<Long> sysIds, ModelLevelType mlt) {
    if (CollectionUtils.isEmpty(sysIds) || mlt == null) {
      return false;
    }
    SqlParams sqlParams = new SqlParams();
    SqlService sqlService = Services.get(SqlService.class);
    sqlParams.setCollectionValue("IDS", sysIds);
    sqlParams.setStringValue("MLT", mlt.getValue());
    sqlService.executeQuery("OMS.EnhancedOrderSqls", "DeleteGridTaskPins", sqlParams);
    return true;
  }
  
  public  static PlatformUserContext getEntAdminContext(EnhancedOrder order,PlatformUserContext platformUserContext) {
    PlatformUserContext adminContext =null;
    UserContextService contextService = Services.get(UserContextService.class);
    //First trying to create Ent Admin context for Requesting Org,if role not available then check for Requesting Org, then for RMO Ent Admin and if ent roles not available then fall for VCAdminContext
    
    if(adminContext == null && !FieldUtil.isNull(order.getOwningOrgEnterpriseName())) {
      try{
        adminContext = contextService.createDefaultEnterpriseAdminContext(platformUserContext.getValueChainId(), order.getOwningOrgEnterpriseName());
        }catch (Exception e) {
          LOG.error("Unable to create Enterprise admin for Owning Org Enterprise "+ order.getOwningOrgEnterpriseName(),e);
      }
    }
    
    if(adminContext == null && !FieldUtil.isNull(order.getBuyingOrgEnterpriseName())) {
      try{
        adminContext = contextService.createDefaultEnterpriseAdminContext(platformUserContext.getValueChainId(), order.getBuyingOrgEnterpriseName());
        }catch (Exception e) {
          LOG.error("Unable to create Enterprise admin for Buying Org Enterprise "+ order.getBuyingOrgEnterpriseName(),e);
      }
    }
    
    if(adminContext == null && !FieldUtil.isNull(order.getOrderMgmtOrgEnterpriseName())) {
      try{
          adminContext = contextService.createDefaultEnterpriseAdminContext(platformUserContext.getValueChainId(), order.getOrderMgmtOrgEnterpriseName());
        }catch (Exception e) {
            LOG.error("Unable to create Enterprise admin for OMO Enterprise "+ order.getOrderMgmtOrgEnterpriseName(),e);
        }
    }
    if(adminContext==null)
      adminContext =contextService.createDefaultValueChainAdminContext(platformUserContext.getValueChainId());
    return adminContext;
  }

  public static void recomputeMilestonesForParentOrder(List<EnhancedOrder> recomputeMilestoneOrder,
		ActionBasedWorkflowContext<EnhancedOrder> context) {
	for(EnhancedOrder order : recomputeMilestoneOrder) {
		OrderTrackingUtil.addActionDrivenTrackingEvent(ListUtil.create(order), context.getPlatformUserContext(), order.getTransientField("ResyncingParentOrder").toString());
	}
  }					
  public static double getPromiseQtyForTransistionalState(String state,List<DeliverySchedule> deliverySchedules,double totalQty) {
	  double qty = 0d;
	  int count = 0;
	  for(DeliverySchedule deliverySchedule:deliverySchedules) {
		  if(!nonTransitionalStates.contains(deliverySchedule.getState())) {
			  qty = qty + deliverySchedule.getPromiseQuantity();
			  count++;
		  }
	  }
	  if(totalQty<qty) {
	    return  Math.round(((qty)/(deliverySchedules.size() - count))*1000)/1000;
	  }
	  return Math.round(((totalQty-qty)/(deliverySchedules.size() - count))*1000)/1000;
  }
  
  public static Map<Long,List<Long>> getReopenedDSIds(EnhancedOrder order, EnhancedOrder currentOrder) {
	  Map<Long,List<Long>> reopenOrderIds = new HashMap<>();
	  List<Long> ids = null;
	 
		  List<RequestSchedule> requestSchedules=OrderUtil.getAllRequestSchedules(order);
		  ids = new ArrayList<Long>();
		  for(RequestSchedule requestSchedule:requestSchedules) {
			 List<DeliverySchedule>  deliverySchedules = requestSchedule.getDeliverySchedules();
			 for(DeliverySchedule deliverySchedule : deliverySchedules) {
				 DeliverySchedule currentDS = OrderUtil.findMatchingDeliverySchedule(currentOrder, deliverySchedule);
				 if(FieldUtil.isNull(deliverySchedule.getVendorRejectReasonCode()) && currentDS.getState().equals(SCCEnhancedOrderConstants.States.VENDOR_REJECTED) ) {
					 ids.add(deliverySchedule.getSysId());
				 }
			 }
		  }	
		  if(!ids.isEmpty()) {
		  reopenOrderIds.put(order.getSysId(), ids);  
		  }
		   
		 
		  return reopenOrderIds;
	  }
  
  
  public static void updateReopenFields(DeliverySchedule deliverySchedule) {
    DeliveryScheduleMDFs omsDsMdfs = deliverySchedule.getMDFs(DeliveryScheduleMDFs.class);
		 deliverySchedule.setState(SCCEnhancedOrderConstants.States.OPEN);
		 deliverySchedule.setAgreedDeliveryDate(DvceConstants.NULL_CALENDAR_VALUE);
		 deliverySchedule.setAgreedIncoDateStartDate(DvceConstants.NULL_CALENDAR_VALUE);
		 deliverySchedule.setAgreedIncoDateStartDate(DvceConstants.NULL_CALENDAR_VALUE);
		 deliverySchedule.setAgreedQuantity(DvceConstants.NULL_DOUBLE_VALUE);
		 deliverySchedule.setAgreedUnitPriceAmount(DvceConstants.NULL_DOUBLE_VALUE);
		 deliverySchedule.setAgreedUnitPriceUOM(DvceConstants.NULL_STRING_VALUE);
		 deliverySchedule.setDsPromiseStatus(DvceConstants.NULL_STRING_VALUE);
		 deliverySchedule.setOrigPromiseDeliveryDate(deliverySchedule.getPromiseDeliveryDate());
		 deliverySchedule.setOrigPromisedQuantity(deliverySchedule.getPromiseQuantity());
		 omsDsMdfs.setAgreedPricePer(DvceConstants.NULL_DOUBLE_VALUE);
	 }
  
  public static String convertListToString(List<String> dataList) {
    String commaSeperatedString="";
    for(String data: dataList) {
      if(FieldUtil.isNull(commaSeperatedString)) {
        commaSeperatedString= data ;     
      }else {
        commaSeperatedString= commaSeperatedString +", "+data;        
      }
    }
    return commaSeperatedString;
  }
  
  public static Double getPromiseUnitPrice(Long dsId) {
    Double promiseUnitPrice = null;
    if(!FieldUtil.isNull(dsId)) {
      SqlParams params = new SqlParams();
      SqlService sqlService = Services.get(SqlService.class);
      params.setLongValue("sys_delivery_schedule_id", dsId);
      SqlResult results = sqlService.executeQuery("OMS.PurchaseOrderUISqls", "getPromiseUnitPrice", params);
      if(!results.getRows().isEmpty() && results.getRows().get(0).getValue("promise_unit_price_amount")!=null){
        promiseUnitPrice = results.getRows().get(0).getDoubleValue("promise_unit_price_amount");
      }
    }
    
    return promiseUnitPrice;
  }
  
  public static Double getPromisePricePer(Long dsId) {
    Double promisePricePer = null;
    if(!FieldUtil.isNull(dsId)) {
      SqlParams params = new SqlParams();
      SqlService sqlService = Services.get(SqlService.class);
      params.setLongValue("sys_delivery_schedule_id", dsId);
      SqlResult results = sqlService.executeQuery("OMS.PurchaseOrderUISqls", "getPromisePricePer", params);
      if(!results.getRows().isEmpty() && results.getRows().get(0).getValue("oms_promise_price_per")!=null){
        promisePricePer = results.getRows().get(0).getDoubleValue("oms_promise_price_per");
      }
    }
    
    return promisePricePer;
  }
  
  public static void updateShipperAndConsigneeStates(List<EnhancedOrder> currentOrders) {
    for (EnhancedOrder enhancedOrder : currentOrders) {
      if (OrderTypeEnum.DEPLOYMENT_ORDER.toString().equals(enhancedOrder.getOrderType())) {
        if (EnhancedOrderConstants.SHIPPER_CHANGE_REQUESTED_STATE.equalsIgnoreCase(enhancedOrder.getState())) {
          enhancedOrder.setState(SCCEnhancedOrderConstants.States.VENDOR_CHANGE_REQUESTED);
        }
        else if (EnhancedOrderConstants.SHIPPER_CONFIRMED_WITH_CHANGES_STATE.equalsIgnoreCase(
          enhancedOrder.getState())) {
          enhancedOrder.setState(SCCEnhancedOrderConstants.States.VENDOR_CONFIRMED_WITH_CHANGES);
        }
        else if (EnhancedOrderConstants.CONSIGNEE_CHANGE_REQUESTED_STATE.equalsIgnoreCase(enhancedOrder.getState())) {
          enhancedOrder.setState(SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED);
        }
        else if (EnhancedOrderConstants.CONSIGNEE_CONFIRMED_WITH_CHANGES_STATE.equalsIgnoreCase(
          enhancedOrder.getState())) {
          enhancedOrder.setState(SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES);
        }

        for (OrderLine orderLine : enhancedOrder.getOrderLines()) {
          if (EnhancedOrderConstants.SHIPPER_CHANGE_REQUESTED_STATE.equalsIgnoreCase(orderLine.getState())) {
            orderLine.setState(SCCEnhancedOrderConstants.States.VENDOR_CHANGE_REQUESTED);
          }
          else if (EnhancedOrderConstants.SHIPPER_CONFIRMED_WITH_CHANGES_STATE.equalsIgnoreCase(orderLine.getState())) {
            orderLine.setState(SCCEnhancedOrderConstants.States.VENDOR_CONFIRMED_WITH_CHANGES);
          }
          else if (EnhancedOrderConstants.CONSIGNEE_CHANGE_REQUESTED_STATE.equalsIgnoreCase(orderLine.getState())) {
            orderLine.setState(SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED);
          }
          else if (EnhancedOrderConstants.CONSIGNEE_CONFIRMED_WITH_CHANGES_STATE.equalsIgnoreCase(
            orderLine.getState())) {
            orderLine.setState(SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES);
          }

          for (RequestSchedule rs : orderLine.getRequestSchedules()) {
            if (EnhancedOrderConstants.SHIPPER_CHANGE_REQUESTED_STATE.equalsIgnoreCase(rs.getState())) {
              rs.setState(SCCEnhancedOrderConstants.States.VENDOR_CHANGE_REQUESTED);
            }
            else if (EnhancedOrderConstants.SHIPPER_CONFIRMED_WITH_CHANGES_STATE.equalsIgnoreCase(rs.getState())) {
              rs.setState(SCCEnhancedOrderConstants.States.VENDOR_CONFIRMED_WITH_CHANGES);
            }
            else if (EnhancedOrderConstants.CONSIGNEE_CHANGE_REQUESTED_STATE.equalsIgnoreCase(rs.getState())) {
              rs.setState(SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED);
            }
            else if (EnhancedOrderConstants.CONSIGNEE_CONFIRMED_WITH_CHANGES_STATE.equalsIgnoreCase(rs.getState())) {
              rs.setState(SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES);
            }

            for (DeliverySchedule ds : rs.getDeliverySchedules()) {
              if (EnhancedOrderConstants.SHIPPER_CHANGE_REQUESTED_STATE.equalsIgnoreCase(ds.getState())) {
                ds.setState(SCCEnhancedOrderConstants.States.VENDOR_CHANGE_REQUESTED);
              }
              else if (EnhancedOrderConstants.SHIPPER_CONFIRMED_WITH_CHANGES_STATE.equalsIgnoreCase(ds.getState())) {
                ds.setState(SCCEnhancedOrderConstants.States.VENDOR_CONFIRMED_WITH_CHANGES);
              }
              else if (EnhancedOrderConstants.CONSIGNEE_CHANGE_REQUESTED_STATE.equalsIgnoreCase(ds.getState())) {
                ds.setState(SCCEnhancedOrderConstants.States.BUYER_CHANGE_REQUESTED);
              }
              else if (EnhancedOrderConstants.CONSIGNEE_CONFIRMED_WITH_CHANGES_STATE.equalsIgnoreCase(ds.getState())) {
                ds.setState(SCCEnhancedOrderConstants.States.BUYER_CONFIRMED_WITH_CHANGES);
              }
            }
          }
        }
      }
    }
  }
  
  /**
   * TODO this method will check if parent order line number present for order. If present then checks if related Natural Keys are correct.
   *
   * @param row
   * @throws CsvTransformException 
   */
  public static boolean validateParentOrderLine(CsvRow row, PlatformUserContext ctx) throws CsvTransformException {
    String auxilaryKey=row.get("ParentOrderLineAuxiliaryKey");
    String owningOrgEnterpriseName=row.get("ParentOrderLineOwningOrgEnterpriseName");
    String orderNumber=row.get("ParentOrderLineOrderNumber");
    String owningOrgName=row.get("ParentOrderLineOwningOrgName");
    String lineNumber=row.get("ParentOrderLineLineNumber");
    try {
    OrganizationRow org = OMSUtil.getOrganization(
      new OrganizationKey(
        ctx.getValueChainId(),
        owningOrgEnterpriseName,
        owningOrgName));
    if(org!=null) {
      long sysOwningOrgId =org.getSysOrgId();
        SccOrderLineRow line= OMSUtil.getOrderLine(new SccOrderLineKey(ctx.getValueChainId(),auxilaryKey,orderNumber,lineNumber,sysOwningOrgId));
        if(line==null)
          throw new Exception();
    }
    else
      throw new Exception("Invalid ParentOrderLineOwningOrgName,ParentOrderLineOwningOrgEnterpriseName");
    }
    catch (Exception e) {
      LOG.error("Error in fetching ParentOrderLine"+e.getMessage());
      throw new CsvTransformException(OrderLine.MODEL_LEVEL_TYPE,"Unable to lookup ParentOrderLine, with ParentOrderLineOrderNumber= "+orderNumber
    +" ,ParentOrderLineOwningOrgName="+owningOrgName+" ,ParentOrderLineOwningOrgEnterpriseName="+owningOrgEnterpriseName+" ,ParentOrderLineAuxiliaryKey="+
    auxilaryKey+" ,ParentOrderLineLineNumber="+lineNumber);
    }
    return true;
  }
  
  /**
   * TODO this method will check if parent order number present for order. If present then checks if related Natural Keys are correct.
   *
   * @param row
   * @throws CsvTransformException 
   */
  public static boolean validateParentOrder(CsvRow row, PlatformUserContext ctx) throws CsvTransformException {
    String auxilaryKey=row.get("ParentOrderAuxiliaryKey");
    String owningOrgEnterpriseName=row.get("ParentOrderOwningOrgEnterpriseName");
    String orderNumber=row.get("ParentOrderOrderNumber");
    String owningOrgName=row.get("ParentOrderOwningOrgName");
    try{
      OrganizationRow org = OMSUtil.getOrganization(
        new OrganizationKey(
          ctx.getValueChainId(),
          owningOrgEnterpriseName,
          owningOrgName));
      if(org!=null) {
        long sysOwningOrgId =org.getSysOrgId();
        SccEnhancedOrderRow order= OMSUtil.getEnhancedOrder(new SccEnhancedOrderKey(ctx.getValueChainId(),auxilaryKey,orderNumber,sysOwningOrgId));
        if(order==null)
          throw new Exception();
     }else {
       throw new Exception("Invalid ParentOrderLineOwningOrgName,ParentOrderLineOwningOrgEnterpriseName");
     }
    }
    catch (Exception e) {
      LOG.error("Error in fetching ParentOrderLine"+e.getMessage());
      throw new CsvTransformException(EnhancedOrder.MODEL_LEVEL_TYPE,"Unable to lookup ParentOrder, with ParentOrderOrderNumber= "+orderNumber
      +" ,ParentOrderOwningOrgName="+owningOrgName+" ,ParentOrderOwningOrgEnterpriseName="+owningOrgEnterpriseName+" ,ParentOrderAuxiliaryKey="+
      auxilaryKey);
    }
    return true;
  }
  
  
  /**
   * Returns the Buyer collaboration action which can be executed on the order based on its state.
   * The action returned may update the state, but it should not progress the order further along
   * the state machine.
   * 
   * Ex:
   *  Open state would return OMS.Buyer Change Request and move an order from Open to Buyer Change Requested
   */
  public static String getBuyerCollaborationAction(EnhancedOrder order) {
    String state = order.getState();
    switch(state) {
      case States.AWAITING_APPROVAL:
        return Actions.UPDATE;
      case States.NEW:
        return Actions.REVISE;
      case States.BUYER_CHANGE_REQUESTED:
        return Actions.BUYER_CHANGE_REQUEST;
      case States.BUYER_CONFIRMED_WITH_CHANGES:
        return Actions.BUYER_CHANGE_REQUEST;
      case States.VENDOR_CHANGE_REQUESTED:
        return Actions.BUYER_CONFIRM;
      case States.VENDOR_CONFIRMED_WITH_CHANGES:
        return Actions.BUYER_CONFIRM;
      case States.OPEN:
        return Actions.BUYER_CHANGE_REQUEST;
    }
    return null;
  }
  
  /**
   * Returns the active quantity that should be considered on the schedule.
   * This method prioritises: Agreed > Promised > Requested
   */
  public static double getQuantity(DeliverySchedule schedule) {
    if(schedule.isSetAgreedQuantity() && !FieldUtil.isNull(schedule.getAgreedQuantity())) {
      return schedule.getAgreedQuantity();
    }
    if(schedule.isSetPromiseQuantity() && !FieldUtil.isNull(schedule.getPromiseQuantity())) {
      return schedule.getPromiseQuantity();
    }
    return schedule.getRequestQuantity();
  }
  
  public static boolean getConsolidatePOPolicyValue(PlatformUserContext pltContext) {
	  String consolidate = ExternalReferenceUtil.getLocalValue(REF_TYPE_CONSOLIDATE_PO, ENABLE_CONSOLIDATE_PO);
	  if(Objects.nonNull(consolidate))
		  return true; 
	  return false;
  }
  
  public static Boolean ignoreIfAVLDescrepancy() {
	  String ignoreDescrepancies = ExternalReferenceUtil.getLocalValue(EnhancedOrderConstants.IGNORE_AVL_DESCREPANCIES_FOR_FULFILLMENT_ORDER, 
			  EnhancedOrderConstants.IGNORE_AVL_DESCREPANCIES_FOR_FULFILLMENT_ORDER);
	  if(Objects.nonNull(ignoreDescrepancies)) {
		  return Boolean.valueOf(ignoreDescrepancies); 
	  }
	  return false;   
  }

  public static double getCapacity(DeliverySchedule ds, PlatformUserContext context) {
    RequestSchedule rs = ds.getParent();
    EnhancedOrder eo = rs.getParent().getParent();
    Long transModeId = rs.getSysRsTransModeId();
    if (FieldUtil.isNull(transModeId)) {
      transModeId = eo.getSysTransModeId();
    }
    if (FieldUtil.isNull(transModeId)) {
      return 0; // No transmode no capacity
    }
    if(!FieldUtil.isNull(ds.getSysShipFromSiteId())) {
      SqlService sqlService = Services.get(SqlService.class);
      SqlParams params = new SqlParams();
      params.setLongValue("TO_SITE_ID", rs.getSysShipToSiteId());
      params.setLongValue("FROM_SITE_ID", ds.getSysShipFromSiteId());
      params.setLongValue("EQUIPMENT_ID", transModeId);
      List<SqlRow> rows = sqlService.executeQuery("OMS.EnhancedOrderSqls", "GetSiteLaneTransModeCapacity", params).getRows();
      if(!rows.isEmpty()) {
        SqlRow row = rows.get(0);
        if(!row.isNull("capacity")) {
          return row.getDoubleValue("capacity");
        }
      }
    }
    EquipmentTypeRow truck = EquipmentTypeCacheManager.getInstance().getEquipmentType(
      transModeId,
      (DvceContext) context);
    Double capacity = truck.getQuantityCapacity();
    if(FieldUtil.isNull(capacity)) {
      return 0;
    }
    return capacity;
  }

  /**
   * Delete the Relavent Schedules
   * @param platformUserContext 
   * @param model 
   *
   */
  public static void deleteRelaventSchedules(List<EnhancedOrder> models,boolean nullifyParentOrderRef ,
		  boolean wipeOutChildOrders , PlatformUserContext platformUserContext) {
	  List<String> deadStates = ListUtil.create(States.CANCELLED,States.VENDOR_REJECTED ,States.DELETED);
	  deadStates.addAll(inFullFillmentStates);
	  Set<EnhancedOrder> eligibleOrders =  OrderUtil.eligibleToUpdateParentOrder(models, true);
	  for(EnhancedOrder model : eligibleOrders) {
    SqlParams params = new SqlParams();
    params.setLongValue("SYS_PARENT_ORDER_ID", model.getSysId());
    params.setStringValue("ORDER_TYPE", OrderTypeEnum.RETURN_ORDER.toString());
		  params.setCollectionValue("STATE", deadStates);
		  List<EnhancedOrder> childOrders = new ArrayList<>();
		  if(nullifyParentOrderRef && wipeOutChildOrders) {
		    childOrders = DirectModelAccess.read(EnhancedOrder.class, platformUserContext,
		      params,
		      ModelQuery.sqlFilter("SYS_PARENT_ORDER_ID in $SYS_PARENT_ORDER_ID$ AND STATE NOT IN $STATE$ AND ORDER_TYPE NOT IN $ORDER_TYPE$"));
		    childOrders.stream().forEach(order->order.setTransientField("isComputeOrder",true));
		    nullifyParentOrderReference(childOrders);
		  } else {
			  if(wipeOutChildOrders) {
				  childOrders = DirectModelAccess.read(EnhancedOrder.class, platformUserContext,
						  params,
						  ModelQuery.sqlFilter("SYS_PARENT_ORDER_ID in $SYS_PARENT_ORDER_ID$ AND STATE NOT IN $STATE$ AND ORDER_TYPE NOT IN $ORDER_TYPE$"));
				  childOrders = filterEligibleOrdersToWipeOut(childOrders,platformUserContext);
			  } else {
				  List<Long> orderLineIds =  model.getOrderLines().stream().map(ol -> ol.getSysId()).collect(Collectors.toList());
				  SqlParams sqlParams = new SqlParams();
				  sqlParams.setCollectionValue("STATE", deadStates);
				  sqlParams.setCollectionValue("SYS_ORDER_LINE_IDs", orderLineIds);
				  sqlParams.setStringValue("ORDER_TYPE", OrderTypeEnum.RETURN_ORDER.toString());
				  childOrders = DirectModelAccess.read(EnhancedOrder.class,platformUserContext,
						  sqlParams,
						  ModelQuery.sqlFilter("SCC_ENHANCED_ORDER.STATE NOT IN $STATE$ AND ORDER_TYPE NOT IN $ORDER_TYPE$"),
						  ModelQueryExtensions.whereChildren(
								  OrderLine.class,
								  ModelQuery.sqlFilter("SCC_ORDER_LINE.SYS_PARENT_ORDER_LINE_ID in $SYS_ORDER_LINE_IDs$ AND SCC_ORDER_LINE.STATE NOT IN $STATE$")));
				  if(Objects.nonNull(childOrders) && !childOrders.isEmpty()) {
					  childOrders = filterEligibleOrdersToWipeOut(childOrders,platformUserContext);
					  List<Long> childOrderIds = childOrders.stream().map(order -> order.getSysId()).collect(Collectors.toList());
					  Map<Long, EnhancedOrder> orders = null;
					  if(!childOrderIds.isEmpty()) {
						  orders = DirectModelAccess.readByIds(EnhancedOrder.class, childOrderIds, platformUserContext);
					  }							  
					  List<EnhancedOrder> eligibleOrdersToWipeOutCompletely = new ArrayList<>();
					  List<EnhancedOrder>  eligibleOrdersToWipeOutByLine = new ArrayList<>();
					  if(Objects.nonNull(orders) && !orders.isEmpty()) {
						  for(EnhancedOrder childOrder : childOrders) {
							  EnhancedOrder order = orders.get(childOrder.getSysId());
							  if(Objects.nonNull(order)) {
								  order.getOrderLines().removeIf(ol -> deadStates.contains(ol.getState()));
								  if(order.getOrderLines().size() == childOrder.getOrderLines().size()) {
									  if(order.getOrderLines().size() > 0) {
										  eligibleOrdersToWipeOutCompletely.add(childOrder);
									  }
								  } else {
									  childOrder.getOrderLines().removeIf(ol -> deadStates.contains(ol.getState()));
									  eligibleOrdersToWipeOutByLine.add(childOrder);
								  }
							  }
						  } 
					  }
					  
					  if(!eligibleOrdersToWipeOutCompletely.isEmpty()) {
						  cancelOrDeleteChildOrders(eligibleOrdersToWipeOutCompletely,false,platformUserContext);
					  }
					  if(!eligibleOrdersToWipeOutByLine.isEmpty()) {
						  cancelOrDeleteChildOrders(eligibleOrdersToWipeOutByLine,true,platformUserContext);
					  }
				  }
				  return ;
			  }
		  }
		  cancelOrDeleteChildOrders(childOrders,false,platformUserContext);
	  }
  }

  private static List<EnhancedOrder> filterEligibleOrdersToWipeOut(List<EnhancedOrder> childOrders,
    PlatformUserContext platformUserContext) {
    List<EnhancedOrder> eligibleOrdersToCancel = new ArrayList<>();
    for(EnhancedOrder order : childOrders) {
      boolean allowCancelCollaboration = false;
      if(!(order.getState().equals(com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.DRAFT) 
        || order.getState().equals(States.AWAITING_APPROVAL))) {
        if(order.getOrderType().equalsIgnoreCase(OrderTypeEnum.PURCHASE_ORDER.toString())) {
          allowCancelCollaboration = TransactionCache.getOrgPolicy(
            Policies.ALLOW_COLLABORATION_ON_CANCEL_PO,
            !FieldUtil.isNull(order.getSysOwningOrgId()) ? order.getSysOwningOrgId() : -1L,
              false,
              platformUserContext);
        }else if (order.getOrderType().equalsIgnoreCase(OrderTypeEnum.DEPLOYMENT_ORDER.toString())){
          allowCancelCollaboration = TransactionCache.getOrgPolicy(
            Policies.ALLOW_COLLABORATION_ON_CANCEL_DO,
            !FieldUtil.isNull(order.getSysOwningOrgId()) ? order.getSysOwningOrgId() : -1L,
              false,
              platformUserContext);
        }

      } 
      if(!allowCancelCollaboration) {
        eligibleOrdersToCancel.add(order);
      }
    }
    return eligibleOrdersToCancel;
  }

private static void cancelOrDeleteChildOrders(List<EnhancedOrder> childOrders,boolean ignoreHeaderAction,
  PlatformUserContext platformUserContext) {
  // Using value chain context as the agents cancel also cancel and where the fulfillment orders may not have correct context
  UserContextService contextService = Services.get(UserContextService.class);
  PlatformUserContext vcAdminCtx = contextService.createDefaultValueChainAdminContext(platformUserContext.getValueChainId());
  childOrders.stream().forEach(order->order.setTransientField("doNotSyncParentOrder", true));
  List<EnhancedOrder> childOrdersAwaiting = childOrders.stream().filter(order->
  (order.getState().equalsIgnoreCase(States.AWAITING_APPROVAL) || 
    order.getState().equalsIgnoreCase(com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.DRAFT))).collect(Collectors.toList());
  List<EnhancedOrder> childOrdersWithOtherStates = childOrders.stream().filter(order->!(order.getState().equalsIgnoreCase(States.AWAITING_APPROVAL) ||
    order.getState().equalsIgnoreCase(com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.DRAFT))).
    collect(Collectors.toList());
  if(ignoreHeaderAction) {
    if(Objects.nonNull(childOrdersAwaiting) && !childOrdersAwaiting.isEmpty()) {
      childOrdersAwaiting.stream().forEach(childOrder -> childOrder.setTransientField("syncChildOrder", true));
      childOrdersAwaiting.stream().flatMap(childOrder -> childOrder.getOrderLines().stream()).
      forEach(ol -> ol.setActionName(SCCEnhancedOrderConstants.Actions.DELETE_LINE));
      ModelDataServiceUtil.writeModels(EnhancedOrder.STANDARD_MODEL_NAME,SCCEnhancedOrderConstants.Actions.DELETE_LINE,
        childOrdersAwaiting, vcAdminCtx); 
    }
    if(Objects.nonNull(childOrdersWithOtherStates) && !childOrdersWithOtherStates.isEmpty()) {
      childOrdersWithOtherStates.stream().flatMap(childOrder -> childOrder.getOrderLines().stream()).
      forEach(ol -> ol.setActionName(SCCEnhancedOrderConstants.Actions.CANCEL_LINE));
      ModelDataServiceUtil.writeModels(EnhancedOrder.STANDARD_MODEL_NAME,SCCEnhancedOrderConstants.Actions.CANCEL_LINE,
        childOrdersWithOtherStates, vcAdminCtx); 
    }
  } else {
    if(Objects.nonNull(childOrdersAwaiting) && !childOrdersAwaiting.isEmpty()) {
      ModelDataServiceUtil.writeModels(EnhancedOrder.STANDARD_MODEL_NAME, com.ordermgmtsystem.oms.mpt.SCCEnhancedOrderConstants.Actions.DELETE,
        childOrdersAwaiting, vcAdminCtx); 
    }
    if(Objects.nonNull(childOrdersWithOtherStates) && !childOrdersWithOtherStates.isEmpty()) {
      ModelDataServiceUtil.writeModels(EnhancedOrder.STANDARD_MODEL_NAME, com.ordermgmtsystem.oms.mpt.SCCEnhancedOrderConstants.Actions.CANCEL, 
        childOrdersWithOtherStates, vcAdminCtx); 
    }
  }
}
    
private static void nullifyParentOrderReference(List<EnhancedOrder> childOrdersAwaiting) {
  for(EnhancedOrder order : childOrdersAwaiting) {
    order.setSysParentOrderId(DvceConstants.NULL_LONG_VALUE,true);
    for(OrderLine ol : order.getOrderLines()) {
      ol.setSysParentOrderLineId(DvceConstants.NULL_LONG_VALUE,true);
      for(RequestSchedule rs : ol.getRequestSchedules()) {
        RequestScheduleMDFs reqSchMDF = rs.getMDFs(RequestScheduleMDFs.class);
        reqSchMDF.setSysParentReqScheduleId(DvceConstants.NULL_LONG_VALUE,true);
      }
    }
  }
}

  /**
   * Delete Relavent schedules
   *
   * @param models
   * @param platformUserContext
   */
  public static void deleteRelaventSchedules(List<EnhancedOrder> models,boolean nullifyParentOrder, PlatformUserContext platformUserContext) {
	  List<String> deadStates = ListUtil.create(States.CANCELLED,States.VENDOR_REJECTED ,States.DELETED);
	  List<Long> eligibleRsIds = new ArrayList<>();
	  List<EnhancedOrder> segregateOrders = new ArrayList<>();
	  Set<EnhancedOrder> eligibleOrders =  OrderUtil.eligibleToUpdateParentOrder(models, true);
	  for(EnhancedOrder order : eligibleOrders) {
		  if(deadStates.contains(order.getState())) {
		    segregateOrders.add(order);
			  continue;
		  }
		  for(OrderLine line : order.getOrderLines()) {
			  for(RequestSchedule reqSch : line.getRequestSchedules()) {
				  if(deadStates.contains(reqSch.getState())) {
					  eligibleRsIds.add(reqSch.getSysId());
				  }
			  }
		  }
	  }
	  if(!segregateOrders.isEmpty()) {
		  OrderUtil.deleteRelaventSchedules(segregateOrders, false,true, platformUserContext);
	  }
	  if(!eligibleRsIds.isEmpty()) {
	    deleteRelaventRequestSchedule(eligibleRsIds,nullifyParentOrder,platformUserContext);
	  }
  }

  /**
   * TODO complete method documentation
   *
   * @param eligibleRsIds
   * @param nullifyParentOrder 
   * @param platformUserContext
   */
  public static void deleteRelaventRequestSchedule(List<Long> eligibleRsIds, boolean nullifyParentOrder, PlatformUserContext platformUserContext) {
    List<String> deadStates = ListUtil.create(States.CANCELLED,States.VENDOR_REJECTED ,States.DELETED);
    deadStates.addAll(inFullFillmentStates);
    SqlParams sqlParams = new SqlParams();
    sqlParams.setCollectionValue("SYS_RS_IDs", eligibleRsIds);
    sqlParams.setCollectionValue("STATE", deadStates);
    sqlParams.setStringValue("ORDER_TYPE", OrderTypeEnum.RETURN_ORDER.toString());
    List<EnhancedOrder> childOrders = DirectModelAccess.read(EnhancedOrder.class,platformUserContext,
        sqlParams,
        ModelQuery.sqlFilter("SCC_ENHANCED_ORDER.STATE NOT IN $STATE$ AND ORDER_TYPE NOT IN $ORDER_TYPE$"),
        ModelQueryExtensions.whereChildren(
            RequestSchedule.class,
            ModelQuery.sqlFilter("SCC_REQUEST_SCHEDULE.OMS_SYS_PARENT_REQ_SCHEDULE_ID in $SYS_RS_IDs$ AND SCC_REQUEST_SCHEDULE.STATE NOT IN $STATE$")));
    if(Objects.nonNull(childOrders) && !childOrders.isEmpty()) {
      childOrders.stream().forEach(order->order.setTransientField("doNotSyncParentOrder", true));
      childOrders = filterEligibleOrdersToWipeOut(childOrders,platformUserContext);
      UserContextService contextService = Services.get(UserContextService.class);
      PlatformUserContext vcAdminCtx = contextService.createDefaultValueChainAdminContext(platformUserContext.getValueChainId());
      List<EnhancedOrder> childOrdersAwaiting = childOrders.stream().filter(order->
      (order.getState().equalsIgnoreCase(States.AWAITING_APPROVAL) || 
          order.getState().equalsIgnoreCase(com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.DRAFT))).collect(Collectors.toList());
      List<EnhancedOrder> childOrdersWithOtherStates = childOrders.stream().filter(order->!(order.getState().equalsIgnoreCase(States.AWAITING_APPROVAL) ||
          order.getState().equalsIgnoreCase(com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.DRAFT))).
          collect(Collectors.toList());
      if(Objects.nonNull(childOrdersAwaiting) && !childOrdersAwaiting.isEmpty()) {
        childOrdersAwaiting.stream().flatMap(childOrder -> childOrder.getOrderLines().stream()).flatMap(childLine -> childLine.getRequestSchedules().stream()).
        forEach(rs -> {
          rs.setActionName(SCCEnhancedOrderConstants.Actions.DELETE_REQUEST_SCHEDULE);
          if(nullifyParentOrder) {
            RequestScheduleMDFs.from(rs).setSysParentReqScheduleId(DvceConstants.NULL_LONG_VALUE, true);
          }
        });
        ModelDataServiceUtil.writeModels(EnhancedOrder.STANDARD_MODEL_NAME,SCCEnhancedOrderConstants.Actions.DELETE_REQUEST_SCHEDULE,
            childOrdersAwaiting, vcAdminCtx); 
      }
      if(Objects.nonNull(childOrdersWithOtherStates) && !childOrdersWithOtherStates.isEmpty()) {
        childOrdersWithOtherStates.stream().flatMap(childOrder -> childOrder.getOrderLines().stream()).flatMap(childLine -> childLine.getRequestSchedules().stream()).
        forEach(rs -> {
          rs.setActionName(SCCEnhancedOrderConstants.Actions.CANCEL_REQUEST_SCHEDULE);
          if(nullifyParentOrder) {
            RequestScheduleMDFs.from(rs).setSysParentReqScheduleId(DvceConstants.NULL_LONG_VALUE, true);
          }
        });
        ModelDataServiceUtil.writeModels(EnhancedOrder.STANDARD_MODEL_NAME,SCCEnhancedOrderConstants.Actions.CANCEL_REQUEST_SCHEDULE,
            childOrdersWithOtherStates, vcAdminCtx); 
      }
    }
    
  }

  /**
   * TODO complete method documentation
   * @param platformUserContext 
   * @param models 
   *
   */
  public static List<EnhancedOrder> consolidateAndCreateFulfillmentOrders(List<EnhancedOrder> models, PlatformUserContext platformUserContext) {
    List<SourceOrderElement> poOrderElementList =  new ArrayList<>();
    List<DeliverySchedule> deleteDS = new ArrayList<>();
    Boolean foundError = false;
    for(EnhancedOrder model : models){
      boolean collabPerDS =  Services.get(PolicyService.class).getPolicyValue(
        OMSConstants.Policies.ENABLE_BUYER_COLLABORATION_PER_DS, Organization.MODEL_LEVEL_TYPE,  model.getSysOwningOrgId(), false,  platformUserContext);
      for(OrderLine ol : model.getOrderLines()) {
        boolean dropShipment = ol.isDropShipment();
        if(!FieldUtil.isNull(ol.getSysItemId())) {
          for(RequestSchedule rs : ol.getRequestSchedules()) {
            SourceOrderElement orderElement = new SourceOrderElement();
            Long siteId = null;
            if(dropShipment) {
              siteId = rs.getSysShipToSiteId();
            }
            orderElement.setItemId(ol.getSysItemId());
            if(!FieldUtil.isNull(siteId)) {
              orderElement.setSiteId(siteId);
            }else {
              orderElement.setSiteAddress(rs.getShipToAddress());
            }
            boolean firstDS = true;
            // Assumption that it should have only one DS for each RS
            for(DeliverySchedule ds : rs.getDeliverySchedules()) {
              boolean dropShip = false;
              if(!dropShipment) {
                dropShip = FieldUtil.isNull(ds.getSysShipFromSiteId()) && FieldUtil.isNull(ds.getShipFromAddress());
                siteId = !FieldUtil.isNull(ds.getSysShipFromSiteId()) ? ds.getSysShipFromSiteId()
                  : rs.getSysShipToSiteId();
                if(!FieldUtil.isNull(siteId)) {
                  orderElement.setSiteId(siteId);
                }else {
                  orderElement.setSiteAddress(ds.getShipFromAddress());
                }
              }
              if(OrderUtil.nonTransitionalStates.contains(ds.getState())) {
                continue;
              }
              if(firstDS) {
                Calendar delDate =ds.getRequestDeliveryDate();
                double quantity = ds.getRequestQuantity();
                if(collabPerDS) {
                  quantity = rs.getDeliverySchedules().stream().filter(delSchd->(!FieldUtil.isNull(delSchd.getRequestQuantity()) && !OrderUtil.nonTransitionalStates.contains(delSchd.getState()))).
                    mapToDouble(delSch->delSch.getRequestQuantity()).sum();
                }
                orderElement.setDeliveryDate(delDate);
                orderElement.setDelSchedule(ds);
                if(dropShipment || dropShip) {
                  orderElement.setDropShipment(true);
                }else {
                  orderElement.setDropShipment(false);
                }
                ds.setPromiseQuantity(0);
                ds.setAgreedQuantity(0);
                ds.setRequestQuantity(quantity);
                ds.setPromiseDeliveryDate(DvceConstants.NULL_CALENDAR_VALUE);
                ds.setAgreedDeliveryDate(DvceConstants.NULL_CALENDAR_VALUE);
                ds.setPromiseShipDate(DvceConstants.NULL_CALENDAR_VALUE);
                ds.setAgreedShipDate(DvceConstants.NULL_CALENDAR_VALUE);
                ds.setDsPromiseStatus(DvceConstants.NULL_STRING_VALUE);
                orderElement.setTotalQuantity(quantity);
//                if(Objects.isNull(ds.getTransientField("ProposedOrderType")) || (Objects.nonNull(ds.getTransientField("ProposedOrderType"))
//                  && ds.getTransientField("ProposedOrderType").equals(OrderTypeEnum.PURCHASE_ORDER.toString()))) {
                  List<AvlLineRow> avlLineRows = SourceOrderUtil.getVendorsFromAvlList(model.getSysSellingOrgId(),ol.getSysItemId(), 
                    siteId, (DvceContext) platformUserContext);
                  if(Objects.isNull(avlLineRows) || avlLineRows.isEmpty()) {
                    foundError = true;
                    model.setError("OMS.enhancedOrder.NoAVLsFoundForSourcing");
                    break;
                  } else {
                    if(!orderElement.setAvlLines(avlLineRows,platformUserContext)) {
                      foundError = true;
                      model.setError("OMS.enhancedOrder.NoAVLsFoundForSourcing");
                      break;
                    }else {
                      poOrderElementList.add(orderElement);
                    }
                  }
//                } else {
//                  doOrderElementList.add(orderElement);
//                }
              }else {
                deleteDS.add(ds);
              }
              firstDS = false;
            }
            rs.getDeliverySchedules().removeIf(delSch->deleteDS.contains(delSch));
          }
        }
      }
    }
    if(foundError) {
      return models;
    }
    
    Map<Long,List<SourceOrderElement>> vendorGrpMap = new HashMap<>();
    List<EnhancedOrder> poOrderList = new ArrayList<>();
    if(!poOrderElementList.isEmpty()) {
      vendorGrpMap = SourceOrderUtil.groupByVendorfromSourceOrderElement(null,poOrderElementList);
      poOrderList  = SourceOrderUtil.createFulfillmentOrderFromSourceOrderElement(models,vendorGrpMap,platformUserContext);
      if(!poOrderList.isEmpty()) {
        OrderUtil.populateTemplateId(poOrderList);
        ModelList<EnhancedOrder> poModelList =  ModelDataServiceUtil.writeModels(EnhancedOrder.STANDARD_MODEL_NAME, Actions.CREATE_PO, poOrderList,true, platformUserContext); 
        if(null != poModelList && !poModelList.getErrors().isEmpty()) {
          setError(models,"Error while fulfilling the order with order type(purchase order)");
        }
      }
      if(!deleteDS.isEmpty()) {
        SqlParams sqlParams = new SqlParams();
        sqlParams.setCollectionValue("sys_delivery_schedule_id", deleteDS.stream().map(ds->ds.getSysId()).collect(Collectors.toSet()));
        SqlService ss = Services.get(SqlService.class);
        ss.executeQuery("delete from scc_delivery_schedule where sys_delivery_schedule_id in $sys_delivery_schedule_id$",sqlParams);
      }
    }
//  List<EnhancedOrder> doOrderList  = new ArrayList<>();
//  if(!doOrderElementList.isEmpty()) {
//    doOrderList = SourceOrderUtil.createFulfillmentOrderFromSourceOrderElement(model,doOrderElementList,platformUserContext);
//  }
//  if(!poOrderList.isEmpty() || !doOrderList.isEmpty()) {
//    if(!poOrderList.isEmpty()) {
//      OrderUtil.populateTemplateId(poOrderList);
//    }
//    if(!doOrderList.isEmpty()) {
//      OrderUtil.populateTemplateId(doOrderList);
//    }
//
//    ModelList<EnhancedOrder> poModelList = null;
//    ModelList<EnhancedOrder> doModelList = null;
//
//    if(poOrderList.size() > 0) {
//      poModelList =  ModelDataServiceUtil.writeModels(EnhancedOrder.STANDARD_MODEL_NAME, Actions.CREATE_PO, poOrderList,true, platformUserContext); 
//    }
//    if(doOrderList.size() > 0) {
//      doModelList =  ModelDataServiceUtil.writeModels(EnhancedOrder.STANDARD_MODEL_NAME, Actions.CREATE_DO, doOrderList,true, platformUserContext); 
//    }
//
//    if(null != poModelList && !poModelList.getErrors().isEmpty()) {
//      model.setError("Error while fulfilling the order with order type(purchase order)");
//    }else {
//      if(!deleteDS.isEmpty()) {
//        SqlParams sqlParams = new SqlParams();
//        sqlParams.setCollectionValue("sys_delivery_schedule_id", deleteDS.stream().map(ds->ds.getSysId()).collect(Collectors.toSet()));
//        SqlService ss = Services.get(SqlService.class);
//        ss.executeQuery("delete from scc_delivery_schedule where sys_delivery_schedule_id in $sys_delivery_schedule_id$",sqlParams);
//      }
//    }
//    if(null != doModelList && !doModelList.getErrors().isEmpty()) {
//      String error = StringUtil.EMPTY_STRING;
//      if(!FieldUtil.isNull(model.getError().toString())) {
//        error = model.getError().toString();
//      } 
//      model.setError(error+"Error while fulfilling the order with order type(Deployment order)");
//    }else {
//      if(!deleteDS.isEmpty()) {
//        SqlParams sqlParams = new SqlParams();
//        sqlParams.setCollectionValue("sys_delivery_schedule_id", deleteDS.stream().map(ds->ds.getSysId()).collect(Collectors.toSet()));
//        SqlService ss = Services.get(SqlService.class);
//        ss.executeQuery("delete from scc_delivery_schedule where sys_delivery_schedule_id in $sys_delivery_schedule_id$",sqlParams);
//      }
//    }
    return models;
  }
  
  public static void setError(List<EnhancedOrder> models, String errorStr) {
    for(EnhancedOrder model : models) {
      model.setError(errorStr);
    }
  }

	public static void setHoldAttributesForOrder(EnhancedOrder order, List<Hold> draftHoldsList) {
		JSONObject attributes = new JSONObject();
		String orderType= OrderRestUtil.getOrderType(order);
		attributes.put("Type", orderType);
		draftHoldsList.stream().forEach(hold -> {
			hold.getMDFs(HoldMDFs.class).setAttributes(attributes.toString());
		});
	}
	
	public static String getOrderOrContractTypeFromHold(Hold hold,PlatformUserContext context) {
		if(hold.getHoldType().equalsIgnoreCase(com.ordermgmtsystem.supplychaincore.model.enums.HoldTypeEnum.ORDER_BUYER_HOLD.toString())
				|| hold.getHoldType().equalsIgnoreCase(com.ordermgmtsystem.supplychaincore.model.enums.HoldTypeEnum.ORDER_SELLER_HOLD.toString())) {
			List<EnhancedOrder> orders = null;
			EnhancedOrder order = null;
			ModelRetrieval modelRetrieval = ModelQuery.retrieve(EnhancedOrder.class);
      modelRetrieval.setIncludeAttachments(EnhancedOrder.class, false);
			if(hold.getTransactionModelLevel().equals(OrderLine.MODEL_LEVEL_TYPE.toString())) {
				orders = DirectModelAccess.read(EnhancedOrder.class,context,
						null,
						ModelQueryExtensions.whereChildren(
								OrderLine.class,
								ModelQueryExtensions.withSysIds(hold.getTransactionId())), modelRetrieval);
				if(!orders.isEmpty()) {
					order = orders.get(0);
				}
			}else if(hold.getTransactionModelLevel().equals(RequestSchedule.MODEL_LEVEL_TYPE.toString())) {
				orders = DirectModelAccess.read(EnhancedOrder.class,context,
						null,
						ModelQueryExtensions.whereChildren(
								RequestSchedule.class,
								ModelQueryExtensions.withSysIds(hold.getTransactionId())), modelRetrieval);
				if(!orders.isEmpty()) {
					order = orders.get(0);
				}
			}else if(hold.getTransactionModelLevel().equals(DeliverySchedule.MODEL_LEVEL_TYPE.toString())) {
				orders = DirectModelAccess.read(EnhancedOrder.class,context,
						null,
						ModelQueryExtensions.whereChildren(
								DeliverySchedule.class,
								ModelQueryExtensions.withSysIds(hold.getTransactionId())), modelRetrieval);
				if(!orders.isEmpty()) {
					order = orders.get(0);
				}
			} else {
				order = DirectModelAccess.readById(EnhancedOrder.class,hold.getTransactionId(),context, modelRetrieval);
			}
			if(Objects.nonNull(order)) {
				return order.getOrderType();
			}
			return OrderTypeEnum.PURCHASE_ORDER.toString();
		}
		else if(hold.getHoldType().equalsIgnoreCase(ContractHoldConstants.HOLD_TYPE)){      
  		Contract contract= null;;
      List<Contract> contracts= null;
      ModelRetrieval modelRetrieval = ModelQuery.retrieve(Contract.class);
      modelRetrieval.setIncludeAttachments(Contract.class, false);
      if(hold.getTransactionModelLevel().equals(ContractLine.MODEL_LEVEL_TYPE.toString())) {      
        if(!FieldUtil.isNull(hold.getTransactionId())) {
          contracts = DirectModelAccess.read(Contract.class,context,
            null,
            ModelQueryExtensions.whereChildren(
              ContractLine.class,
              ModelQueryExtensions.withSysIds(hold.getTransactionId())), modelRetrieval);
          if(!contracts.isEmpty()) {
            contract = contracts.get(0);
          }
        }
        
      } else {
        if(!FieldUtil.isNull(hold.getTransactionId())) {
          contract  = ModelDataServiceUtil.readModelById(Contract.class, hold.getTransactionId(), context);
        }         
      }
      if(Objects.nonNull(contract)) {        
        return contract.getContractType();
      }
      return ContractConstants.CONTRACT_TYPE_PROCUREMENT;
		} 
		else
			return null;
	}
	
	
	public static CostComponent buildCostCompForPlanningShippingCost(DeliverySchedule ds,AdditionalCost addlCost,SiteLaneTransMode siteLaneMode, Double totalAmt,String currency) {
		  CostComponent cost = new CostComponent();
		  cost.setChargeType(AdditionalCostChargeTypeEnum.PLANNEDSHIPPINGCOST.toString());
		  cost.setChargeLevelId(ds.getSysId());
		  cost.setDescription(AdditionalCostChargeTypeEnum.PLANNEDSHIPPINGCOST.toString());
		  cost.setSourceId(siteLaneMode.getSysId());
		  cost.setSourceModelLevel(SiteLaneTransMode.MODEL_LEVEL_TYPE.toString());
		  cost.setChargeLevelModelLevel(DeliverySchedule.MODEL_LEVEL_TYPE.toString());
		  cost.setChargeLevelDisplay(OrderUtil.getTransactionDisplay(ds));
		  cost.setCostAmount(totalAmt);
		  cost.setCostUOM(currency);
		  cost.setDistributed(false);
		  cost.setDeleted(false);
		  cost.setDistributionCode(LandedCostDistributionTypeEnum.NONE.toString());
		  addlCost.getCostComponents().add(cost);
		  return cost;
	}

  /**
   * TODO complete method documentation
   *
   * @param list
   * @return 
   */
  public static List<EnhancedOrder> getCleanOrderToPopulateFromDB(List<EnhancedOrder> orders) {
    List<EnhancedOrder> cleanOrders = new ArrayList<>();
    for(EnhancedOrder order : orders) {
      if(!FieldUtil.isNull(order.getSysId())) {
        EnhancedOrder cleanOrder = new EnhancedOrder();
        cleanOrder.setSysId(order.getSysId());
        for(OrderLine ol : order.getOrderLines()) {
          OrderLine newOl = new OrderLine();
          newOl.setSysId(ol.getSysId());
          newOl.setParent(cleanOrder);
          for(RequestSchedule rs : ol.getRequestSchedules()) {
            RequestSchedule newRs = new RequestSchedule();
            newRs.setSysId(rs.getSysId());
            newRs.setParent(newOl);
            for(DeliverySchedule ds : rs.getDeliverySchedules()) {
              DeliverySchedule newDs = new DeliverySchedule();
              newDs.setSysId(ds.getSysId());
              newDs.setParent(newRs);
              newRs.getDeliverySchedules().add(newDs);
            }
            newOl.getRequestSchedules().add(newRs);
          }
          cleanOrder.getOrderLines().add(newOl);
        }
        cleanOrders.add(cleanOrder);
      } else {
        cleanOrders.add(order);
      }
    }
    return cleanOrders;
  }

  /**
   * TODO complete method documentation
   *
   * @param cleanOrders
   * @param platformUserContext
   */
  public static void deletePackingRequirements(List<EnhancedOrder> cleanOrders, PlatformUserContext platformUserContext) {
    for(EnhancedOrder order :cleanOrders){
      String packingAlgoSet = Services.get(PolicyService.class).getPolicyValue(SCCConstants.Policies.PACKING_RESOURCE_ALGORITHM_TYPE, 
        Organization.MODEL_LEVEL_TYPE, order.getSysBuyingOrgId(), null, platformUserContext);
      if(FieldUtil.isNull(packingAlgoSet)) {
        continue;
      }
      OrderUtil.forEachDeliverySchedule(order, (ds->{
        if(deadStates.contains(ds.getState()))
          PackingResourceUtil.getInstance().deletePackingRequirements(ds.getSysId(), DeliverySchedule.MODEL_LEVEL_TYPE, HoldTypeEnum.ORDER_BUYER_HOLD.toString(),
              Services.get(UserContextService.class).createDefaultValueChainAdminContext(platformUserContext.getValueChainId()));
      }));
    }
  }
  
  public static List<String> getStatesForPromiseShortProblem(){
    String value = ExternalReferenceUtil.getLocalValue("PromiseShortProblemStates", "Restrict");
    if(value!=null && value =="true" ) {
      return OPW_RESTRICTED_STATES;
    } else {
      return OPW_STATES;
    }
  }
  
  /**
   * Validate policy to see if cancel collaboration is allowed
   *
   * @param order - The Enhanced Order 
   * @param ctx - The PlatformUserContext
   * @return boolean - true if the policy is enabled, false otherwise
   */
  public static boolean isCancelCollaborationAllowed(EnhancedOrder order, PlatformUserContext context) {
    boolean allowCancelCollaboration = false;
    if(null == order) {
      return allowCancelCollaboration;
    }

    String policyName = null;
    if(OrderTypeEnum.PURCHASE_ORDER.toString().equals(order.getOrderType())) {
      policyName = Policies.ALLOW_COLLABORATION_ON_CANCEL_PO;
    }
    else if (OrderTypeEnum.DEPLOYMENT_ORDER.toString().equals(order.getOrderType())){
      policyName = Policies.ALLOW_COLLABORATION_ON_CANCEL_DO;
    }

    if(null != policyName) {
      allowCancelCollaboration =  TransactionCache.getOrgPolicy(
        policyName,
        !FieldUtil.isNull(order.getSysOwningOrgId()) ? order.getSysOwningOrgId() : -1L,
        false,
        context);
    }

    return allowCancelCollaboration;
  }
  
  /**
   * Validate policy to see if Production Order Creation is allowed
   *
   * @param order - The Enhanced Order 
   * @param ctx - The PlatformUserContext
   * @return boolean - true if the policy is enabled, false otherwise
   */
  public static boolean isProdOrderCreationPolicyEnabled(EnhancedOrder order, PlatformUserContext context) {
    boolean isProdOrderCreationPolicyEnabled = false;
    if(null == order) {
      return isProdOrderCreationPolicyEnabled;
    }

    if(OrderTypeEnum.DEPLOYMENT_ORDER.toString().equals(order.getOrderType())
      && !FieldUtil.isNull(order.getSysShipFromOrgId())) {
      isProdOrderCreationPolicyEnabled =  TransactionCache.getOrgPolicy(
        OMSConstants.Policies.ENABLE_PRODUCTION_ORDER_CREATION,
        order.getSysShipFromOrgId(),
        false,
        context);
    }
    else if(!FieldUtil.isNull(order.getSysSellingOrgId())) {
      isProdOrderCreationPolicyEnabled =  TransactionCache.getOrgPolicy(
        OMSConstants.Policies.ENABLE_PRODUCTION_ORDER_CREATION,
        order.getSysSellingOrgId(),
        false,
        context);
    }

    return isProdOrderCreationPolicyEnabled;
  }
    
  /**
   * This method is to trigger manufacturing MFG.InsertOrUpdateProductionOrder event.
   * @param context
   */
  public static void triggerInsertOrUpdateProductionOrderEvent(
    List<Long> sysDeliveryScheduleIds,
    Long creationOrgId,
    Long executionOrgId) throws Exception {
    PlatformEventService evtSvc = Services.get(PlatformEventService.class);
    PlatformEvent productionOrderInsertOrUpdateEvent = new PlatformEvent(OmsConstants.MFG_INSERT_OR_UPDATE_PRODUCTION_ORDER_EVENT);
    try {
      productionOrderInsertOrUpdateEvent.getParameters().put(OmsConstants.SYS_DS_IDs, sysDeliveryScheduleIds);
      productionOrderInsertOrUpdateEvent.getParameters().put(OmsConstants.CREATION_ORG_ID, creationOrgId);
      productionOrderInsertOrUpdateEvent.getParameters().put(OmsConstants.EXECUTION_ORG_ID, executionOrgId);
      evtSvc.fireEvent(productionOrderInsertOrUpdateEvent);
    }
    catch (Exception e) {
      LOG.debug("Exeception occure in triggerInsertOrUpdateProductionOrderEvent : " + e.getStackTrace());
      throw new Exception ("Exeception occure in triggerInsertOrUpdateProductionOrderEvent.");
    }
  }
  
  /**
   * This method is to trigger manufacturing MFG.CancelProductionOrder event.
   * @param context
   */
  public static void triggerCancelProductionOrderEvent(List<Long> sysDeliveryScheduleIds, Long creationOrgId) 
    throws Exception{
    PlatformEventService evtSvc = Services.get(PlatformEventService.class);
    PlatformEvent cancelProductionOrderEvent = new PlatformEvent(OmsConstants.MFG_CANCEL_PRODUCTION_ORDER_EVENT);
    try {
      cancelProductionOrderEvent.getParameters().put(OmsConstants.SYS_DS_IDs, sysDeliveryScheduleIds);
      cancelProductionOrderEvent.getParameters().put(OmsConstants.CREATION_ORG_ID, creationOrgId);
      evtSvc.fireEvent(cancelProductionOrderEvent);
    }
    catch (Exception e) {
      LOG.debug("Exeception occure in triggerCancelProductionOrderEvent : " + e.getStackTrace());
      throw new Exception ("Exeception occure in triggerCancelProductionOrderEven.");
    }
  }

  /**
   * populate the site org on order
   *
   * @param order
   * @param ctx
   */
  public static void populateSiteOrgOnEnhancedOrder(EnhancedOrder order, PlatformUserContext ctx) {
    if(FieldUtil.isNull(order.getSysShipToOrgId())) {
      OrderUtil.getAllRequestSchedules(order).forEach(rs -> {
        if(!FieldUtil.isNull(rs.getSysRsShipToOrgId())) {
          order.setSysShipToOrgId(rs.getSysRsShipToOrgId(), true);
        }
      });
    }
    if(FieldUtil.isNull(order.getSysShipFromOrgId())) {
      OrderUtil.forEachDeliverySchedule(
        order,
        ds -> {
          if(!FieldUtil.isNull(ds.getSysDsShipFromOrgId())) {
            order.setSysShipFromOrgId(ds.getSysDsShipFromOrgId(), true);
          }
        });
    }
    
  }
  /**
   * populate the owning org on order
   *
   * @param order
   * @param ctx
   */
  public static void populateOwningOrgonEnhancedOrder(EnhancedOrder order, PlatformUserContext ctx){
    if(FieldUtil.isNull(order.getOwningOrgName()) && !FieldUtil.isNull(order.getSysOwningOrgId())) {
      OrganizationRow org = OrganizationCacheManager.getInstance().getOrganization(order.getSysOwningOrgId());
      order.setOwningOrgEnterpriseName(org.getEntName());
      order.setOwningOrgName(org.getOrgName());
    }
    
  }
  
  public  static PlatformUserContext getContextForPromiseShortPrescription(PlatformUserContext platformUserContext) {
    Long userId = platformUserContext.getUserId();
    Long vcId = platformUserContext.getValueChainId();
    UserContextService contextService = Services.get(UserContextService.class);
    List<UserAssociation> usros = null;
    try {
      usros = getAllUSROForUser(userId, vcId, platformUserContext);
    } catch(Exception e) {
      LOG.error("Error Faced while fetching USRO - "+e.getMessage());
    }
    
    if(usros!=null && !usros.isEmpty()) {
      for(UserAssociation usro:usros) {
        PlatformUserContext ctx  = contextService.createContext(usro.getSysUserId(), usro.getSysRoleId());
        if(ctx.isDerivedFrom(SCCConstants.RoleTypes.BUYER_SUPPLY_CHAIN_PLANNER)) {
          return ctx;
        }
      }
    }
    
    return null;
   
     
    
  }
  
  private static PlatformUserContext getEnterpriseAdminContext(PlatformUserContext platformUserContext) {
    PlatformUserContext adminContext =null;
    UserContextService contextService = Services.get(UserContextService.class);
    if(adminContext == null && !FieldUtil.isNull(platformUserContext.getRoleEnterpriseName())) {
      try{
        adminContext = contextService.createDefaultEnterpriseAdminContext(platformUserContext.getValueChainId(), platformUserContext.getRoleEnterpriseName());
        }catch (Exception e) {
          LOG.error("Unable to create Enterprise admin for platformUserContext.getRoleEnterpriseName() "+ platformUserContext.getRoleEnterpriseName(),e);
      }
    }    
    
    if(adminContext==null)
      adminContext =contextService.createDefaultValueChainAdminContext(platformUserContext.getValueChainId());
    return adminContext;
  }
  
  private static List<UserAssociation> getAllUSROForUser(Long userId, Long vcId, PlatformUserContext platformUserContext) throws Exception  {
    
    ModelDataService mds = Services.get(ModelDataService.class);
    SqlParams params = new SqlParams();
    params.setLongValue("userId", userId);
    params.setLongValue("vcId", vcId);
    StringBuffer query = new StringBuffer();
    query.append("usro.sys_user_id = $userId$ and usro.user_vc_id = $vcId$  and exists(select 1 from role where role.sys_role_id = usro.sys_role_id and role.is_active=1) ");
    List<UserAssociation> usros = mds.read(
      UserAssociation.class,
      platformUserContext,
      params,
      ModelQuery.sqlFilter(query.toString()));
    return usros.stream().filter(usro->usro.isActive()).collect(Collectors.toList());
    
  }
        
  public static void setDropShipFromBufferOrItem(OrderLine line, PlatformUserContext ctx) {
	  boolean isPurchaseOrder = line.getParent().getOrderType().equals(OrderTypeEnum.PURCHASE_ORDER.toString()) ? true : false;
	  boolean isDeploymentOrder = line.getParent().getOrderType().equals(OrderTypeEnum.DEPLOYMENT_ORDER.toString()) ? true : false;
	  Long siteId = null;
	  Long locationId = null;
	  Boolean isDropFlag = null;
	  if(isPurchaseOrder || isDeploymentOrder) {
		  List<RequestSchedule> reqSchedules = OrderUtil.getAllRequestSchedules(line);
		  RequestSchedule reqSchedule =  reqSchedules.stream().filter(reqSch -> (!OrderUtil.newNonTransitionalStates.contains(reqSch.getState())
				  && !FieldUtil.isNull(reqSch.getSysShipToSiteId()))).findFirst().orElse(null);
		  if(Objects.nonNull(reqSchedule)) {
			  siteId = reqSchedule.getSysShipToSiteId();
			  locationId = reqSchedule.getSysShipToLocationId();
		  }
	  } else {
		  List<DeliverySchedule> delSchedules = OrderUtil.getAllDeliverySchedules(line);
		  DeliverySchedule delSchedule =  delSchedules.stream().filter(delSch -> (!OrderUtil.newNonTransitionalStates.contains(delSch.getState())
				  && !FieldUtil.isNull(delSch.getSysShipFromSiteId()))).findFirst().orElse(null);
		  if(Objects.nonNull(delSchedule)) {
			  siteId = delSchedule.getSysShipFromSiteId();
			  locationId = delSchedule.getSysShipFromLocationId();
		  }
	  }
	  if(Objects.nonNull(siteId)) {
		  isDropFlag = OrderUtil.getDropShipFromBufferOrItem(line.getSysItemId(), siteId , locationId, null, ctx);
	  }else {
		  isDropFlag = OrderUtil.getDropShipFromBufferOrItem(line.getSysItemId(), null, null,null, ctx);
	  }
	  if(Objects.nonNull(isDropFlag)) {
		  line.setDropShipment(isDropFlag);
	  }
  }
  
  public static Boolean getDropShipFromBufferOrItem(Long itemId , Long siteId,Long locationId, Long contractLineId ,PlatformUserContext ctx) {
	  if(!FieldUtil.isNull(contractLineId)) {
      ModelRetrieval modelRetrieval = ModelQuery.retrieve(ContractLine.class);
      modelRetrieval.setIncludeAttachments(ContractLine.class, false);
		  ContractLine contractLine = DirectModelAccess.readById(ContractLine.class,contractLineId, ctx, modelRetrieval);
		  if(Objects.nonNull(contractLine)) {
			  return contractLine.isIsDropShip();
		  } else {
			  return OrderUtil.getDropShipFromBufferOrItem(itemId, siteId, locationId, null, ctx);
		  }
	  }else if(Objects.nonNull(itemId)) {
		  if(!FieldUtil.isNull(siteId)) {
			  Buffer buffer = TransactionCache.getBuffer(itemId, siteId, locationId, ctx);
			  if(Objects.nonNull(buffer) && BufferDetailMDFs.from(buffer).isSetIsDropShip()) {
				  return BufferDetailMDFs.from(buffer).isIsDropShip();
			  }
		  }
		  Item item = TransactionCache.getItem(itemId, ctx);
		  if(Objects.nonNull(item) && ItemMDFs.from(item).isSetIsDropShip()) {
			  return ItemMDFs.from(item).isIsDropShip(); 
		  }
	  }
	  return null;
  }
  
  //To check if Financial features enabled
  public static boolean isFinancialsEnabled(Long entId) {
    Set<Feature> subscriptions = FeatureSubscriptionCacheManager.getInstance().getSubscriptions(entId);
    boolean disabled = false;
    if (Objects.isNull(subscriptions)) {
      return false;
    }

    for (Feature subscription : subscriptions) {
      if (subscription.getName().equals(OMSConstants.Features.FINANCIALS)) {
        return true;
      }
    }
    return disabled;
  }

  //To get enterprise for which financials feature is enabled
  public static List<Long> getFeatureEnabledEnterpriseIds(String featureName, PlatformUserContext context) {
    List<Long> entIds = null;
    SqlParams sqlParams = new SqlParams();
    sqlParams.setStringValue("FEATURE", featureName);
    String sql = "FEATURE_SUBSCRIPTION.FEATURE = $FEATURE$";
    ModelRetrieval modelRetrieval = ModelQuery.retrieve(FeatureSubscription.class);
    modelRetrieval.setIncludeAttachments(FeatureSubscription.class, false);
    List<FeatureSubscription> subscriptions = DirectModelAccess.read(
      FeatureSubscription.class,
      context,
      sqlParams,
      ModelQuery.sqlFilter(sql), modelRetrieval);
    if (!subscriptions.isEmpty()) {
      entIds = subscriptions.stream().map(s -> s.getSysEnterpriseId()).collect(Collectors.toList());
    }
    return entIds;
  }

  public static Map<Integer, List<Long>> getSelectedRowsInChunk(
    int chunkSize,
    String sqlQuery,
    SqlParams params,
    PlatformUserContext context) {
    try (PSRLoggerEntry psr = new PSRLoggerEntry("Order", null, "getSelectedRowsInChunk")) {
      SqlService s = Services.get(SqlService.class);
      Map<Integer, List<Long>> orderMap = new HashMap<Integer, List<Long>>();
      int key = 1;
      s.executeQuery(new SqlQueryRequest(sqlQuery, params), new AbstractSqlResultHandler() {
        List nestedList = new ArrayList<Long>();

        @Override
        public void processSqlRow(SqlRow row) throws SqlResultHandlerException {

          if (nestedList.size() == chunkSize) {
            if (orderMap.isEmpty()) {
              orderMap.put(key, nestedList);
            }
            else {
              int size = orderMap.size();
              orderMap.put(size + 1, nestedList);
            }
            nestedList = new ArrayList<Long>();
          }
          if (!row.isNull("SYS_ENHANCED_ORDER_ID")) {
            nestedList.add(row.getLongValue("SYS_ENHANCED_ORDER_ID"));
          }
        }

        @Override
        public void endSqlResult() throws SqlResultHandlerException {
          if (orderMap.isEmpty() && nestedList.size() >= 0) {
            orderMap.put(key, nestedList);
          }
        }
      });
      return orderMap.isEmpty() ? null : orderMap;
    }
  }

  public static Map<RequestSchedule, AvlLine> preloadAVLs(List<RequestSchedule> rsList, PlatformUserContext ctx) {
    rsList = rsList.stream().filter(r -> {
     String orderType = r.getParent().getParent().getOrderType();
     if(OrderTypeEnum.DEPLOYMENT_ORDER.toString().equalsIgnoreCase(orderType) ||
       OrderTypeEnum.SALES_ORDER.toString().equalsIgnoreCase(orderType)) {
       return false;
     }
     return true;
    }).collect(Collectors.toList());
    if (rsList != null && !rsList.isEmpty())
      return OrderUtil.getAVLLineFromRS(rsList, ctx);
    else
      return null;
  }

  public static Map<RequestSchedule, AvlLine> preloadAVLFromOrders(
    List<EnhancedOrder> orders,
    PlatformUserContext ctx) {
    List<RequestSchedule> rsList = new ArrayList<RequestSchedule>();
    for (EnhancedOrder order : orders) {
      if (!order.isIsSpot() && !OrderTypeEnum.DEPLOYMENT_ORDER.toString().equalsIgnoreCase(order.getOrderType())) {
        rsList.addAll(OrderUtil.getAllRequestSchedules(order));
      }

    }
    if (rsList != null && !rsList.isEmpty())
      return OrderUtil.getAVLLineFromRS(rsList, ctx);
    else
      return null;
  }

  public static Map<RequestSchedule, Buffer> preloadBuffers(List<EnhancedOrder> orders, PlatformUserContext ctx) {
    return OrderUtil.getBufferPerRS(orders, ctx);
  }

  public static Map<String, List<OmsOrgProcurmntPolicyRow>> preloadOrgProcPolicy(
    List<EnhancedOrder> orders,
    PlatformUserContext ctx) {
    List<String> propertyNames = new ArrayList<String>();
    propertyNames.addAll(
      Arrays.asList(
        OrgProcPolicyConstants.PO_NO_ASSIGNMENT,
        OrgProcPolicyConstants.PO_LINE_NO_ASSIGNMENT,
        OrgProcPolicyConstants.AUTO_RECEIPT_DO,
        OrgProcPolicyConstants.IGNORE_DATE_COMPLIANCE_TIME_CHECK,
        OrgProcPolicyConstants.AUTO_MOVE_TO_RECEIPT_DO,
        OrgProcPolicyConstants.DEFAULT_SHIPFROMSITE_FROM_ORG,
        OrgProcPolicyConstants.LIMIT_ORDERS_PER_CONTRACT_TERM,
        OrgProcPolicyConstants.ACCEPT_UPLOADED_ORDER_NUMBER,
        OrgProcPolicyConstants.RECALCULATE_PLANNED_DEL_DATE,
        OrgProcPolicyConstants.SHIPMENT_NUMBER_POLICY,
        OrgProcPolicyConstants.PIV_PROJECTED_ON_HAND_ENGINE_NAME,
        OrgProcPolicyConstants.PURCHASE_ORDER_EDIT_FIELDS,
        OrgProcPolicyConstants.ACCEPT_USER_PROVIDED_UNIT_PRICE,
        OrgProcPolicyConstants.POPULATE_DEFAULT_REQUEST_SHIP_DATE,
        OrgProcPolicyConstants.CALCULATE_ORDER_TOTALS));
    List<String> orderTypePropertyNames = new ArrayList<String>();
    orderTypePropertyNames.addAll(Arrays.asList(OrgProcPolicyConstants.VALIDATE_DUPLICATE_ITEM_POLICY_PREFIX));
    for (EnhancedOrder order : orders) {
      orderTypePropertyNames = orderTypePropertyNames.stream().map(
        p -> OrderUtil.getPolicyNameForOrderType(p, order.getOrderType())).collect(Collectors.toList());
      propertyNames.addAll(orderTypePropertyNames);

    }
    return OrgProcPolicyUtil.getMultiplePolicyValueForMultipleOrgs(propertyNames, orders, (DvceContext) ctx);
  }
  
	public static void mergeAndUpdateOrderState(List<EnhancedOrder> orders, List<EnhancedOrder> currentDBOrders,PlatformUserContext ctx) {
    mergeAndUpdateOrderState(orders, currentDBOrders,false, ctx);
  }
  
  public static void mergeAndUpdateOrderState(List<EnhancedOrder> orderFromDB, List<EnhancedOrder> inputOrders,boolean isBuyer,PlatformUserContext ctx) {
    for (EnhancedOrder order : orderFromDB) {
      EnhancedOrder currentOrder = ModelUtil.findMatching(order, inputOrders);
      boolean collabPerDS = TransactionCache.getOrgPolicy(
        OMSConstants.Policies.ENABLE_BUYER_COLLABORATION_PER_DS, currentOrder.getSysOwningOrgId(), false, ctx);
			if(Objects.nonNull(currentOrder)) {
				for(OrderLine currLine : currentOrder.getOrderLines()) {
					OrderLine ol = ModelUtil.findMatching(currLine, order.getOrderLines());
					if(Objects.nonNull(ol)) {
						ol.setState(currLine.getState());
						for(RequestSchedule currRS : currLine.getRequestSchedules()) {
							RequestSchedule rs = ModelUtil.findMatching(currRS, ol.getRequestSchedules());
							if(Objects.nonNull(rs)) {
								rs.setState(currRS.getState());
							}
              DeliverySchedule appDs = null;
              if(Objects.nonNull(rs)) {
							for(DeliverySchedule currDs : currRS.getDeliverySchedules()) {
							  DeliverySchedule ds = ModelUtil.findMatching(currDs, rs.getDeliverySchedules());
							  if(Objects.nonNull(ds)) {
							    ds.setState(currDs.getState());
                    appDs = currDs;
							  }
							}
						}
              if(Objects.nonNull(appDs) && isBuyer && !collabPerDS) {
                List<DeliverySchedule> eligibleDS = new ArrayList<>();
                for(DeliverySchedule inputDS : rs.getDeliverySchedules()) {
                  DeliverySchedule ds = ModelUtil.findMatching(inputDS, currRS.getDeliverySchedules());
                  if(Objects.isNull(ds)) {
                    //copy the request fields 
                    inputDS.setRequestQuantity(appDs.getRequestQuantity());
                    inputDS.setRequestDeliveryDate((Calendar)appDs.getRequestDeliveryDate().clone());
                    if(!FieldUtil.isNull(appDs.getRequestShipDate())) {
                      inputDS.setRequestDeliveryDate((Calendar)appDs.getRequestShipDate().clone());
                    }
                    if(!FieldUtil.isNull(appDs.getRequestShipDate())) {
                      inputDS.setRequestDeliveryDate((Calendar)appDs.getRequestShipDate().clone());
                    }
                    if(!FieldUtil.isNull(appDs.getRequestIncoDateStartDate())) {
                      inputDS.setRequestIncoDateStartDate((Calendar)appDs.getRequestIncoDateStartDate().clone());
                    }
                    if(!FieldUtil.isNull(appDs.getRequestIncoDateEndDate())) {
                      inputDS.setRequestIncoDateEndDate((Calendar)appDs.getRequestIncoDateEndDate().clone());
                    }
                    if(!FieldUtil.isNull(appDs.getRequestMinItemExpiryDate())) {
                      inputDS.setRequestMinItemExpiryDate((Calendar)appDs.getRequestMinItemExpiryDate().clone());
                    }
                    JSONObject json = inputDS.toJSONObject();
                    DeliverySchedule clonedDelSch = Model.fromJSONObject(json, DeliverySchedule.class);
                    clonedDelSch.setParent(currRS);
                    eligibleDS.add(clonedDelSch);
                  }
                }
                if(!eligibleDS.isEmpty()) {
                  currRS.getDeliverySchedules().addAll(eligibleDS);
                }
              }
            }
					ol.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(ol)));
					currLine.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(ol)));
          }
					}
				currentOrder.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(order)));
			} 
		}
	}

	/**
   * Method to fetch AutoCreateFulfillmentOrder for Order based on OrderType
   * PO refers to AVL and VendorMaster
   * SO refers to ACL and CustomerMaster
   * DO based on Policy 
   *
   * @param order
   * @param ctx
   * @return
   */
  @SuppressWarnings("unchecked")
  public static String fetchAutoCreateFulfillmentOrderBasedOnOrderType(
    EnhancedOrder order,
    PlatformUserContext ctx) {
    
    String autoCreateFulfillmentOrder = null;
    String psrKey = null;
    try {
      if (PSRLogger.isEnabled()) {
        psrKey = PSRLogger.enter(PSR_ID + " fetchAutoCreateFulfillmentOrderBasedOnOrderType");
      }
      
      OrderTypeEnum orderType = OrderTypeEnum.get(order.getOrderType());
      if(OrderTypeEnum.BLANKET_ORDER == orderType) { 
        if( Objects.equals(order.getSysBuyingOrgId(),order.getSysSellingOrgId())) {
          orderType = OrderTypeEnum.DEPLOYMENT_ORDER;
        }
        else {
          orderType = OrderTypeEnum.PURCHASE_ORDER;
        }
      }
      if(isContract(order)) {
        autoCreateFulfillmentOrder = getAutoCreateFOFromVendor(order, ctx, autoCreateFulfillmentOrder);
      }
      else if (OrderTypeEnum.PURCHASE_ORDER == orderType) {
        //-- [PURCHASE_ORDER type will use AVL or VendorMaster for AutoCreateFulfillmentOrder value]
      List<OrderLine> lines = new ArrayList<OrderLine>();
      lines.addAll(order.getOrderLines());
      EnhancedOrder dbOrder= DirectModelAccess.readById(EnhancedOrder.class, order.getSysId(), ctx); 
      if(dbOrder!=null) { 
        lines.clear();
        lines.addAll(dbOrder.getOrderLines()); 
      }
      line: for (OrderLine line : lines) {
          if (!order.isIsSpot()) { // This condition is added to respect the Vendor master for AutoCreateFulfillmentOrder policy for SpotPO
            if (!FieldUtil.isNull(line.getSysItemId())) {
              for (RequestSchedule rs : line.getRequestSchedules()) {
                if (OrderUtil.nonTransitionalStates.contains(rs.getState())) {
                  continue;
                }
                AvlLine avlLine = OrderUtil.getAVLFromRS(rs, ctx);
                if (avlLine != null) {
                  AvlLineMDFs avlLineMDFs = avlLine.getMDFs(AvlLineMDFs.class);
                  if (!FieldUtil.isNull(avlLineMDFs.getAutoCreateFulfillmentOrder())) {
                    autoCreateFulfillmentOrder = autoCreateFulfillmentOrder == null
                      ? avlLineMDFs.getAutoCreateFulfillmentOrder()
                      : autoCreateFulfillmentOrder;
                    if (autoCreateFulfillmentOrder != null) { //-- If found, break the loop at Line level
                      break line;
                    }
                  }
                }
              }
            }
          }
          if (autoCreateFulfillmentOrder == null ) {
            autoCreateFulfillmentOrder = getAutoCreateFOFromVendor(order, ctx, autoCreateFulfillmentOrder);
          }
        }
      }
      else if (OrderTypeEnum.SALES_ORDER == orderType) {
        //-- [SALES_ORDER type will use ACL or CustomerMaster for  value]
        Map<Long, AclLine> itemToAclLine = OrderUtil.getItemToAclLineMap(order, ctx);
        if(itemToAclLine!=null && !itemToAclLine.isEmpty()) {
          for(AclLine aclLine:itemToAclLine.values()) {
            if (aclLine != null) {
              AclLineMDFs aclLineMDFs = aclLine.getMDFs(AclLineMDFs.class);
              autoCreateFulfillmentOrder = autoCreateFulfillmentOrder == null ? aclLineMDFs.getAutoCreateFulfillmentOrder() : autoCreateFulfillmentOrder;
              
            }else { //--This condition is valid for Spot-SO as well.
              autoCreateFulfillmentOrder = getAutoCreateFulfillmentOrderCustomerValue(order);
            }
          }  
        }if (autoCreateFulfillmentOrder == null ) { //--This condition is valid for Spot-SO as well.
          autoCreateFulfillmentOrder = getAutoCreateFulfillmentOrderCustomerValue(order);
        }
        
      }
        else { //-- Condition for DEPLOYMENT_ORDER 
            Policy<String> policy = Policy.AUTO_CREATE_FULFILLMENT_ORDER;
            if (isSingleShipFromSite(order)) {
            Long siteId =null;
              for (OrderLine line : order.getOrderLines()) {
                for (RequestSchedule rs : line.getRequestSchedules()) {
                  for (DeliverySchedule ds : rs.getDeliverySchedules()) {
                    if (!FieldUtil.isNull(ds.getSysShipFromSiteId())) {
                      siteId = ds.getSysShipFromSiteId();
                      break;
                    }
                  }
                }
              } 
              if(Objects.nonNull(siteId)) {
                autoCreateFulfillmentOrder= TransactionCache.getSitePolicy(policy.getName(), siteId, policy.getDefaultValue(), ctx);
              }
          }
          
        }
              
      

      return autoCreateFulfillmentOrder;
    }finally {
      if (PSRLogger.isEnabled())
        PSRLogger.exit(psrKey);
    }
  }

  /**
   * method to get autoCreateFulfillmentOrder value from vendor master
   *
   * @param order
   * @param ctx
   * @param autoCreateFulfillmentOrder
   * @return
   */
  private static String getAutoCreateFOFromVendor(
    EnhancedOrder order,
    PlatformUserContext ctx,
    String autoCreateFulfillmentOrder) {
    //--This condition is valid for Spot-PO as well.
      PartnerRow partnerRow=null;
      if (!FieldUtil.isNull(order.getSysVendorId())) {
        partnerRow = PartnerUtil.getPartner(order.getSysVendorId(), ctx);
      }
      else {
        PartnerKey partnerKey = new PartnerKey(
          order.getVendorName(),
          order.getValueChainId(),
          order.getVendorEnterpriseName(),
          order.getVendorOrganizationName());
        partnerRow = PartnerUtil.getPartner(partnerKey, ctx);
      }
      //-- If valid Partner Row is returned from either of above cases, fetch the autoCreateShipmentValue from the row
      if (partnerRow != null) {
        if (partnerRow.isIsActive()) {
          autoCreateFulfillmentOrder=partnerRow.getOmsAutoCreateFulfillmentOr();
        }
        else {
          order.setError("OMS.enhancedOrder.InactivePartnerShip", new Object[] { partnerRow.getPartnerName() });
        }
      }
    return autoCreateFulfillmentOrder;
  }
  /**
   * TODO complete method documentation
   *
   * @param order
   * @return
   */
  private static String getAutoCreateFulfillmentOrderCustomerValue(EnhancedOrder order) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "getAutoCreateShipmentCustomerValue")) {
      if (order == null)
        return null;
      String autoCreateFulfillmentOrder=null;
      if (!FieldUtil.isNull(order.getSysCustomerId())) {
        PartnerRow row = PartnerUtil.getPartner(order.getSysCustomerId(), null);
        if (row != null) {
          autoCreateFulfillmentOrder=row.getOmsAutoCreateFulfillmentOr();
        }
        return autoCreateFulfillmentOrder;
      }
      else if (!FieldUtil.isNull(order.getCustomerEnterpriseName())
        && !FieldUtil.isNull(order.getCustomerOrganizationName()) && !FieldUtil.isNull(order.getCustomerName())) {
        PartnerKey key = new PartnerKey();
        key.setVcId(order.getValueChainId());
        key.setEntName(order.getCustomerEnterpriseName());
        key.setOrgName(order.getCustomerOrganizationName());
        key.setPartnerName(order.getCustomerName());
        PartnerRow row = PartnerUtil.getPartner(key, null);
        if (row != null) {
          autoCreateFulfillmentOrder=row.getOmsAutoCreateFulfillmentOr();
          return autoCreateFulfillmentOrder;
        }
        return autoCreateFulfillmentOrder;
      }
      else {
        Long tempBuyingOrgId = getOrgByKeyOptionally(
          order.getSysBuyingOrgId(),
          order.getBuyingOrgEnterpriseName(),
          order.getBuyingOrgName(),
          order.getValueChainId());
        Long tempSellingOrgId = getOrgByKeyOptionally(
          order.getSysSellingOrgId(),
          order.getSellingOrgEnterpriseName(),
          order.getSellingOrgName(),
          order.getValueChainId());

        PartnerRow row = PartnerUtil.getCustomerMasterRow(order.getValueChainId(), tempSellingOrgId, tempBuyingOrgId);
        if (row != null) {
          autoCreateFulfillmentOrder=row.getOmsAutoCreateFulfillmentOr();
          return autoCreateFulfillmentOrder;
        }
      }
      return autoCreateFulfillmentOrder;
    }
  }
  
  public static String getMyItem(Long itemId, String entName) {
    SqlService service = Services.get(SqlService.class);
    SqlParams params = new SqlParams();
    params.setLongValue("SYS_ITEM_ID", itemId);
    params.setStringValue("ENT_NAME", entName);
    SqlResult result = service.executeQuery("select  pkg_scc_util.getMyItemName($SYS_ITEM_ID$,$ENT_NAME$) name from dual", params);
    if (!result.getRows().isEmpty()) {
      String myItems = result.getRows().get(0).getStringValue("name");
      return myItems;
    }
    return null;
  }
  
  public static String getMappedItem(Long itemId, String entName) {
    SqlService sqlService = Services.get(SqlService.class);
    SqlParams params = new SqlParams();
    params.setLongValue("IN_ITEM_ID", itemId);
    params.setStringValue("IN_MY_ENT_NAME", entName);
    SqlResult results = sqlService.executeQuery("OMS.AutocompleteSqls", "GetMappedItemForPromiseItem", params);
    if (!results.getRows().isEmpty()) {
      String myItemids = results.getRows().get(0).getStringValue("name");
      return myItemids;
    }
    return null;
  }
  
  public static Long getMatchingPGLItems(Long entId, String entName, Long orgId, Long promiseItemId, Long pglId, Long vcid) {
    if(FieldUtil.isNull(promiseItemId) || FieldUtil.isNull(entName)) {
      return null;
    }
    SqlParams sqlParams = new SqlParams();
    SqlService sqlService = Services.get(SqlService.class);
    sqlParams.setLongValue("ENT_ID", entId);
    sqlParams.setStringValue("ENT_NAME", entName);
    sqlParams.setLongValue("ORG_ID", orgId);
    sqlParams.setLongValue("PGL_ID", pglId);
    sqlParams.setLongValue("MY_VC_ID", vcid);
    SqlResult sqlResult = sqlService.executeQuery("OMS.AutocompleteSqls", "GetPGLItems", sqlParams);    
    if (!sqlResult.getRows().isEmpty()) {
      for(int i=0;i<sqlResult.getRows().size();i++) {
        Long itemId = sqlResult.getRows().get(i).getLongValue("sys_item_id");
        if(!FieldUtil.isNull(itemId) && itemId.equals(promiseItemId)) {
          return itemId;
        }
      }
    }
    String mappedItems =  getMappedItem(promiseItemId, entName);
    List<Long> itemIds = new ArrayList<Long>();
    if(!FieldUtil.isNull(mappedItems)) {
      String[] mappedItemArray = mappedItems.split(",");
      for(String mappedItem:mappedItemArray) {
        Long id = Long.parseLong(mappedItem);
        if(!itemIds.contains(id) && !id.equals(promiseItemId)) {
          itemIds.add(id);
        }
      }
    }
    if(itemIds.isEmpty() || itemIds.size()>1) {
      return null;
    }
    if (!sqlResult.getRows().isEmpty()) {
      for(int i=0;i<sqlResult.getRows().size();i++) {
        Long itemId = sqlResult.getRows().get(i).getLongValue("sys_item_id");
        String name = sqlResult.getRows().get(i).getStringValue("itemname");
        if(!FieldUtil.isNull(itemId) && !FieldUtil.isNull(name) && !itemIds.isEmpty()) {    
          if(itemIds.contains(itemId)) {
            return itemId;
          }
        } 
      }
      
    }
    return null;
  }

  /**
   * Get a map of EnumFieldName->EnumName for a specific model
   */
  public static Map<String,String> getModelEnumFields(ModelLevelType level) {
    Map<String,String> fieldNameToEnumType = new HashMap<>();
    
    ModelType modelType = ORMappingCacheManager.getInstance().getModelType(level);
    ORMappingHelper orMappingHelper = ORMappingHelper.getORMappingHelper(modelType);
    LevelMappingHelper levelHelper = orMappingHelper.getLevelMappingHelper(level);
    for (ILevelChildMappingHelper childMappingHelper : levelHelper.getLevelChildMappingHelpersByNormalcy(true)) {
      ModelFieldMapping modelFieldMapping = childMappingHelper.getModelFieldMapping();
      FieldType fieldType = modelFieldMapping.getFieldType();
      String fieldName = childMappingHelper.getMappingDisplayName();
      
      if(fieldType != null && fieldType.equals(CoreFieldType.STRING_ENUMERATION)) {
        FieldMapping fieldMapping = (FieldMapping) modelFieldMapping;
        EnumerationType enumType = fieldMapping.getEnumerationType();
        Enum enumTypeOther = fieldMapping.getEnum();
    
        String enumName  = (enumType != null) ? enumType.toString() : "";
        if(enumName.length() == 0 && enumTypeOther.getType() != null) 
          enumName = enumTypeOther.getType().toString();
        
        fieldNameToEnumType.put(fieldName, enumName);
      }
    }
    return fieldNameToEnumType;
  }
  
  public static String getActionNameToSyncParentOrder(String orderType) {
    String actionName = SCCEnhancedOrderConstants.Actions.SYNCH_SO;
    switch(orderType){
    case "Purchase Order" :{
      actionName =  SCCEnhancedOrderConstants.Actions.SYNCH_PO;
      break;
    }case "Deployment Order":{
      actionName =  SCCEnhancedOrderConstants.Actions.SYNCH_DO;
      break;
    }default :
      break;
    }
    return actionName;
  
  }
  
  public static boolean getDateRangePoulatePolicyValue(PlatformUserContext pltContext) {
    String isEnabled=ExternalReferenceUtil.getLocalValue(ShipmentConstants.REF_TYPE_ENABLE_SHIPMENT_WINDOW_FOR_PREASN, ShipmentConstants.ENABLE_SHIPMENT_WINDOW_FOR_PREASN);
    if(isEnabled!=null )
      return Boolean.valueOf(isEnabled); 
     return false;
  }
  
  public static boolean getIsCompareWithExtPolicyValue() {
    String isEnabled=ExternalReferenceUtil.getLocalValue(ShipmentConstants.IS_COMPARE_WITH_EXT_SITE_NAMES, ShipmentConstants.IS_COMPARE_WITH_EXT_SITE_NAMES);
    if(isEnabled!=null )
      return Boolean.valueOf(isEnabled); 
    return true;
  }
  
	public static List<PurchaseOrderFact> convertOrderToFact(List<EnhancedOrder> orders, DvceContext context) {
		DvceContext vcDvceContext = ValueChainCacheManager.getInstance()
				.cloneDefaultVcAdminContext(context.getValueChainId());
		for (EnhancedOrder order : orders) {
			List<PurchaseOrderFact> poOrderFact = new ArrayList<PurchaseOrderFact>();
			List<SalesOrderFact> soOrderFact = new ArrayList<SalesOrderFact>();
			List<DeploymentOrderFact> doOrderFact = new ArrayList<DeploymentOrderFact>();
			OrganizationRow buyingOrg = null;
			if (!FieldUtil.isNull(order.getSysBuyingOrgId())) {
				buyingOrg = OrganizationCacheManager.getInstance().getOrganization(order.getSysBuyingOrgId(),
						vcDvceContext);
			}
			OrganizationRow sellingOrg = null;
			if (!FieldUtil.isNull(order.getSysSellingOrgId())) {
				sellingOrg = OrganizationCacheManager.getInstance().getOrganization(order.getSysSellingOrgId(),
						vcDvceContext);
			}
			OrganizationRow owningOrg = null;
			if (!FieldUtil.isNull(order.getSysOwningOrgId())) {
				owningOrg = OrganizationCacheManager.getInstance().getOrganization(order.getSysOwningOrgId(),
						vcDvceContext);
			}
			OrganizationRow custOfBuyerOrg = null;
			if (!FieldUtil.isNull(order.getSysCustOfBuyerOrgId())) {
				custOfBuyerOrg = OrganizationCacheManager.getInstance().getOrganization(order.getSysCustOfBuyerOrgId(),
						vcDvceContext);
			}
			OrganizationRow omoOrg = null;
			if (!FieldUtil.isNull(order.getSysOrderMgmtOrgId()))
				omoOrg = OrganizationCacheManager.getInstance().getOrganization(order.getSysOrderMgmtOrgId(),
						vcDvceContext);

			OrganizationRow fulFillmentOrg = null;
			if (!FieldUtil.isNull(order.getSysFulfillmentOrgId()))
				fulFillmentOrg = OrganizationCacheManager.getInstance().getOrganization(order.getSysFulfillmentOrgId(),
						vcDvceContext);
			OrganizationRow shipFromOrg = null;
			if (!FieldUtil.isNull(order.getSysShipFromOrgId()))
				shipFromOrg = OrganizationCacheManager.getInstance().getOrganization(order.getSysShipFromOrgId(),
						vcDvceContext);
			OrganizationRow shipToOrg = null;
			if (!FieldUtil.isNull(order.getSysShipToOrgId()))
				shipToOrg = OrganizationCacheManager.getInstance().getOrganization(order.getSysShipToOrgId(),
						vcDvceContext);
			OrganizationRow tcoOrg = null;
			if (!FieldUtil.isNull(order.getSysTCOOrgId()))
				tcoOrg = OrganizationCacheManager.getInstance().getOrganization(order.getSysTCOOrgId(), vcDvceContext);
			OrganizationRow freightFwdOrg = null;
			if (!FieldUtil.isNull(order.getSysFreightFwdOrgId()))
				freightFwdOrg = OrganizationCacheManager.getInstance().getOrganization(order.getSysFreightFwdOrgId(),
						vcDvceContext);
			EnhancedOrderMDFs orderMdfs = order.getMDFs(EnhancedOrderMDFs.class);
			for (OrderLine line : order.getOrderLines()) {
				OrderLineMDFs lineMdfs = line.getMDFs(OrderLineMDFs.class);
				ProductGroupLevelRow pglrRow = null;
				if (!FieldUtil.isNull(line.getSysProductGroupLevelId())) {
					pglrRow = ProductGroupLevelCacheManager.getInstance()
							.getProductGroupLevel(line.getSysProductGroupLevelId(), vcDvceContext);
				}

				ItemRow itemRow = null;
				if (!FieldUtil.isNull(line.getSysItemId())) {
					itemRow = ItemCacheManager.getInstance().getItem(line.getSysItemId());
				}
				PartnerRow partnerRow = null;
				if (!FieldUtil.isNull(order.getSysVendorId())) {
					partnerRow = PartnerCacheManager.getInstance().getPartner(order.getSysVendorId(), vcDvceContext);
				}
				for (RequestSchedule rs : line.getRequestSchedules()) {

					RequestScheduleMDFs rsMdfs = rs.getMDFs(RequestScheduleMDFs.class);
					SiteRow shipToSite = null;
					if (!FieldUtil.isNull(rs.getSysShipToSiteId())) {
						shipToSite = SiteCacheManager.getInstance().getSite(rs.getSysShipToSiteId(), vcDvceContext);
					}
					for (DeliverySchedule ds : rs.getDeliverySchedules()) {
						if (OrderTypeEnum.PURCHASE_ORDER.stringValue().equals(order.getOrderType())) {
							PurchaseOrderFact fact = new PurchaseOrderFact();
							fact.setOrderId(order.getSysId());
							fact.setLineId(line.getSysId());
							fact.setRequestScheduleId(rs.getSysId());
							fact.setDeliveryScheduleId(ds.getSysId());
							fact.setOrderNumber(order.getOrderNumber());
							fact.setLineNumber(line.getLineNumber());
							fact.setRequestScheduleNumber(rs.getRequestScheduleNumber());
							fact.setDeliveryScheduleNumber(ds.getDeliveryScheduleNumber());
							if (!FieldUtil.isNull(line.getSpecificItemName()))
								fact.setSpecificItemName(line.getSpecificItemName());
							else
								fact.setSpecificItemName(DvceConstants.NULL_STRING_VALUE);
							if (!FieldUtil.isNull(line.getGenericItemName()))
								fact.setGenericItemName(line.getGenericItemName());
							else
								fact.setGenericItemName(DvceConstants.NULL_STRING_VALUE);
							if (!FieldUtil.isNull(ds.getSysShipFromSiteId())) {
								fact.setShipFromSiteId(ds.getSysShipFromSiteId());
								SiteRow shipFromSiteRow = SiteCacheManager.getInstance()
										.getSite(ds.getSysShipFromSiteId(), vcDvceContext);
								fact.setShipFromSiteName(shipFromSiteRow.getSiteName());
								fact.setShipFromDescription(shipFromSiteRow.getDescription());
								fact.setShipFromIsDC(shipFromSiteRow.isIsDc());
								fact.setShipFromIsPlant(shipFromSiteRow.isIsPlant());
							} else {
								fact.unsetShipFromSiteId();
								fact.setShipFromSiteName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromDescription(DvceConstants.NULL_STRING_VALUE);
								fact.unsetShipFromIsDC();
								fact.unsetShipFromIsPlant();
							}
							if (!FieldUtil.isNull(rs.getSysShipToSiteId())) {
								SiteRow shipToSiteRow = SiteCacheManager.getInstance().getSite(rs.getSysShipToSiteId(),
										vcDvceContext);
								fact.setShipToSiteId(rs.getSysShipToSiteId());
								fact.setShipToDescription(shipToSiteRow.getDescription());
								fact.setShipToSiteName(shipToSiteRow.getSiteName());
								fact.setShiptoSiteIsPlant(shipToSiteRow.isIsDc());
								fact.setShiptoSiteIsDC(shipToSiteRow.isIsPlant());
							} else {
								fact.unsetShipToSiteId();
								fact.setShipToDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToSiteName(DvceConstants.NULL_STRING_VALUE);
								fact.unsetShiptoSiteIsPlant();
								fact.unsetShiptoSiteIsDC();
							}
							if (buyingOrg != null) {
								fact.setBuyingOrgId(order.getSysBuyingOrgId());
								fact.setBuyingOrgDescription(buyingOrg.getDescription());
								fact.setBuyingOrgName(buyingOrg.getOrgName());
								fact.setBuyingOrgEntName(buyingOrg.getEntName());
							} else {
								fact.unsetBuyingOrgId();
								fact.setBuyingOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setBuyingOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setBuyingOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (sellingOrg != null) {
								fact.setSellingOrgId(order.getSysSellingOrgId());
								fact.setSellingOrgDescription(sellingOrg.getDescription());
								fact.setSellingOrgName(sellingOrg.getOrgName());
								fact.setSellingOrgEntName(sellingOrg.getEntName());
							} else {
								fact.unsetSellingOrgId();
								fact.setSellingOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setSellingOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setSellingOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (owningOrg != null) {
								fact.setOwningOrgId(order.getSysOwningOrgId());
								fact.setOwningOrgDescription(owningOrg.getDescription());
								fact.setOwningOrgName(owningOrg.getOrgName());
								fact.setOwningOrgEntName(owningOrg.getEntName());
							} else {
								fact.unsetOwningOrgId();
								fact.setOwningOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setOwningOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setOwningOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (custOfBuyerOrg != null) {
								fact.setCustomerOfBuyerOrgId(order.getSysCustOfBuyerOrgId());
								fact.setCustomerOfBuyerDescription(custOfBuyerOrg.getDescription());
								fact.setCustOfBuyerOrgName(custOfBuyerOrg.getOrgName());
								fact.setCustOfBuyerOrgEntName(custOfBuyerOrg.getEntName());
							} else {
								fact.unsetCustomerOfBuyerOrgId();
								fact.setCustomerOfBuyerDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setCustOfBuyerOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setCustOfBuyerOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (omoOrg != null) {
								fact.setOMOOrgId(order.getSysOrderMgmtOrgId());
								fact.setOMOOrgDescription(omoOrg.getDescription());
								fact.setOMOOrgName(omoOrg.getOrgName());
								fact.setOMOOrgEntName(omoOrg.getEntName());
							} else {
								fact.unsetOMOOrgId();
								fact.setOMOOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setOMOOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setOMOOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (fulFillmentOrg != null) {
								fact.setFulfillmentOrgId(order.getSysFulfillmentOrgId());
								fact.setFulfillmentOrgDescription(fulFillmentOrg.getDescription());
								fact.setFulfillmentOrgName(fulFillmentOrg.getOrgName());
								fact.setFulfillmentOrgEntName(fulFillmentOrg.getEntName());
							} else {
								fact.unsetFulfillmentOrgId();
								fact.setFulfillmentOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setFulfillmentOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setFulfillmentOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (freightFwdOrg != null) {
								fact.setFreightFwdOrgId(order.getSysFreightFwdOrgId());
								fact.setFreightFwdOrgDescription(freightFwdOrg.getDescription());
								fact.setFreightFwdOrgName(freightFwdOrg.getOrgName());
								fact.setFreightFwdOrgEntName(freightFwdOrg.getEntName());
							} else {
								fact.unsetFreightFwdOrgId();
								fact.setFreightFwdOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setFreightFwdOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setFreightFwdOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (shipFromOrg != null) {
								fact.setShipFromOrgId(order.getSysShipFromOrgId());
								fact.setShipFromOrgDescription(shipFromOrg.getDescription());
								fact.setShipFromOrgName(shipFromOrg.getOrgName());
								fact.setShipFromOrgEntName(shipFromOrg.getEntName());
							} else {
								fact.unsetShipFromOrgId();
								fact.setShipFromOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (shipToOrg != null) {
								fact.setShipToOrgId(order.getSysShipToOrgId());
								fact.setShipToOrgDescription(shipToOrg.getDescription());
								fact.setShipToOrgName(shipToOrg.getOrgName());
								fact.setShipToOrgEntName(shipToOrg.getEntName());
							} else {
								fact.unsetShipToOrgId();
								fact.setShipToOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}

              if (!FieldUtil.isNull(rs.getSysRsShipToOrgId())) {
							fact.setRsShipToOrgId(rs.getSysRsShipToOrgId());
              } else {
                fact.setRsShipToOrgId(DvceConstants.NULL_LONG_VALUE);
              }
              if (!FieldUtil.isNull(rs.getRsShipToOrgName())) {
							fact.setRsShipToOrgName(rs.getRsShipToOrgName());
              } else {
                fact.setRsShipToOrgName(DvceConstants.NULL_STRING_VALUE);
              }
              if (!FieldUtil.isNull(rs.getShipToAddress()))
                 fact.setShipToAddress(rs.getShipToAddress());
              else {
                fact.setShipToAddress(DvceConstants.NULL_ADDRESS_VALUE);
              }                 
              if (!FieldUtil.isNull(ds.getShipFromAddress()))
                 fact.setShipFromAddress(ds.getShipFromAddress());
              else {
                 fact.setShipFromAddress(DvceConstants.NULL_ADDRESS_VALUE);
              }  
							if (tcoOrg != null) {
								fact.setTCOOrgId(order.getSysTCOOrgId());
								fact.setTCOrgDescription(tcoOrg.getDescription());
								fact.setTCOrgName(tcoOrg.getOrgName());
								fact.setTCOrgEntName(tcoOrg.getEntName());
							} else {
								fact.unsetTCOOrgId();
								fact.setTCOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setTCOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setTCOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (itemRow != null) {
								fact.setItemId(line.getSysItemId());
								fact.setItemDescription(itemRow.getDescription());
								fact.setItemName(itemRow.getItemName());
								fact.setItemEnterpriseName(itemRow.getEntName());
								fact.setExtMfgItemName(itemRow.getExtMfgItemName());
							} else {
								fact.unsetItemId();
								fact.setItemDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setItemName(DvceConstants.NULL_STRING_VALUE);
								fact.setItemEnterpriseName(DvceConstants.NULL_STRING_VALUE);
								fact.setExtMfgItemName(DvceConstants.NULL_STRING_VALUE);
							}
							if (partnerRow != null) {
								fact.setVendorId(order.getSysVendorId());
								fact.setVendorDescription(partnerRow.getDescription());
								fact.setVendorName(partnerRow.getPartnerName());
								fact.setVendorOrgName(partnerRow.getPartnerOrgName());
							} else {
								fact.unsetVendorId();
								fact.setVendorDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setVendorName(DvceConstants.NULL_STRING_VALUE);
								fact.setVendorName(DvceConstants.NULL_STRING_VALUE);
							}
							fact.setRemitToAddress(order.getRemitToAddress());
							fact.setBillToAddress(order.getBillToAddress());
							fact.setLineType(line.getLineType());
							if (!FieldUtil.isNull(line.getUnitPrice()))
								fact.setUnitPrice(line.getUnitPrice());
							else
								fact.unsetUnitPrice();
							fact.setSellingAgent1(order.getSellingAgent1Name());
							fact.setSellingAgent2(order.getSellingAgent2Name());
							fact.setSellingAgent3(order.getSellingAgent3Name());
							fact.setSellingAgent4(order.getSellingAgent4Name());
							fact.setBuyingAgent1(order.getBuyingAgent1Name());
							fact.setBuyingAgent2(order.getBuyingAgent2Name());
							fact.setBuyingAgent3(order.getBuyingAgent3Name());
							fact.setBuyingAgent4(order.getBuyingAgent4Name());
							fact.setSellingAgent1EntName(order.getSellingAgent1EnterpriseName());
							fact.setSellingAgent2EntName(order.getSellingAgent2EnterpriseName());
							fact.setSellingAgent3EntName(order.getSellingAgent3EnterpriseName());
							fact.setSellingAgent4EntName(order.getSellingAgent4EnterpriseName());
							fact.setBuyingAgent1EntName(order.getBuyingAgent1EnterpriseName());
							fact.setBuyingAgent2EntName(order.getBuyingAgent2EnterpriseName());
							fact.setBuyingAgent3EntName(order.getBuyingAgent3EnterpriseName());
							fact.setBuyingAgent4EntName(order.getBuyingAgent4EnterpriseName());
							fact.setIsPendingAuthorization(orderMdfs.isIsPendingAuthorization());
							if (!FieldUtil.isNull(line.getSysVendorItemId()))
								fact.setVendorItemId(line.getSysVendorItemId());
							else
								fact.unsetVendorItemId();
							if (!FieldUtil.isNull(line.getSysGenericItemId()))
								fact.setGenericItemId(line.getSysGenericItemId());
							else
								fact.unsetGenericItemId();
							if (!FieldUtil.isNull(line.getSysSpecificItemId()))
								fact.setSpecificItemId(line.getSysSpecificItemId());
							else
								fact.unsetSpecificItemId();
							if (!FieldUtil.isNull(order.getSysSellingAgent1Id()))
								fact.setSellingAgent1Id(order.getSysSellingAgent1Id());
							else
								fact.unsetSellingAgent1Id();
							if (!FieldUtil.isNull(order.getSysSellingAgent2Id()))
								fact.setSellingAgent2Id(order.getSysSellingAgent2Id());
							else
								fact.unsetSellingAgent2Id();
							if (!FieldUtil.isNull(order.getSysSellingAgent3Id()))
								fact.setSellingAgent3Id(order.getSysSellingAgent3Id());
							else
								fact.unsetSellingAgent3Id();
							if (!FieldUtil.isNull(order.getSysSellingAgent4Id()))
								fact.setSellingAgent4Id(order.getSysSellingAgent4Id());
							else
								fact.unsetSellingAgent4Id();
							if (!FieldUtil.isNull(order.getSysBuyingAgent1Id()))
								fact.setBuyingAgent1Id(order.getSysBuyingAgent1Id());
							else
								fact.unsetBuyingAgent1Id();
							if (!FieldUtil.isNull(order.getSysBuyingAgent2Id()))
								fact.setBuyingAgent2Id(order.getSysBuyingAgent2Id());
							else
								fact.unsetBuyingAgent2Id();
							if (!FieldUtil.isNull(order.getSysBuyingAgent3Id()))
								fact.setBuyingAgent3Id(order.getSysBuyingAgent3Id());
							else
								fact.unsetBuyingAgent3Id();
							if (!FieldUtil.isNull(order.getSysBuyingAgent4Id()))
								fact.setBuyingAgent4Id(order.getSysBuyingAgent4Id());
							else
								fact.unsetBuyingAgent4Id();
							if (!FieldUtil.isNull(ds.getSysDsShipFromOrgId()))
								fact.setDsShipFromOrgId(ds.getSysDsShipFromOrgId());
							else
								fact.unsetDsShipFromOrgId();
							DeliveryScheduleMDFs dsMdfs = ds.getMDFs(DeliveryScheduleMDFs.class);
							if (!FieldUtil.isNull(order.getSysClonedFromOrderId()))
								fact.setClonedFromOrderId(order.getSysClonedFromOrderId());
							else
								fact.unsetClonedFromOrderId();
							fact.setOrderSubType(order.getOrderSubType());
							fact.setVendorRejectReasonCode(ds.getVendorRejectReasonCode());
							fact.setOrigRequestQtyUOM(rs.getOriginalRequestQuantityUOM());
							fact.setSalesOrderNumber(order.getSalesOrderNumber());
							fact.setPurchaseOrderNumber(order.getPurchaseOrderNumber());
							fact.setOMOOrderNumber(order.getOmoOrderNumber());
							fact.setWMSOrderNumber(order.getWmsOrderNumber());
							fact.setSubmitForApprovalDate(order.getSubmitForApprovalDate());
							fact.setDeliveryGroupNumber(ds.getDeliveryGroupNumber());
							if (!FieldUtil.isNull(order.getSysTransModeId()))
								fact.setTransModeId(order.getSysTransModeId());
							else
								fact.unsetTransModeId();
							if (!FieldUtil.isNull(rs.getSysRsTransModeId()))
								fact.setRsTransModeId(rs.getSysRsTransModeId());
							else
								fact.unsetRsTransModeId();
							// Date fields
							if (!FieldUtil.isNull(order.getSysCreationOrgId()))
								fact.setCreationOrgId(order.getSysCreationOrgId());
							fact.setRequestDeliveryDate(ds.getRequestDeliveryDate());
							fact.setPromiseDeliveryDate(ds.getPromiseDeliveryDate());
							fact.setASNCreationDate(ds.getASNCreationDate());
							fact.setCreationDate(order.getCreationDate());
							fact.setTransMode(order.getTransModeName());
							fact.setIncoTerms(order.getIncoTerms());
							fact.setExtItemName(line.getExtItemName());
							fact.setBuyerOrderApprovalDate(order.getBuyerOrderApprovalDate());
							fact.setOrigPromiseDeliveryDate(ds.getOrigPromiseDeliveryDate());
							fact.setOrigPromiseShipDate(ds.getOrigPromiseShipDate());
							fact.setOrigRequestShipDate(rs.getOrigRequestShipDate());
							fact.setOrigRequestDeliveryDate(rs.getOrigRequestDeliveryDate());
							fact.setOverriddenDeliveryDate(ds.getOverriddenDeliveryDate());
							fact.setOverriddenShipDate(ds.getOverriddenShipDate());
							fact.setPlannedShipDate(ds.getPlannedShipDate());
							fact.setWhseReleaseTargetDate(ds.getWhseReleaseTargetDate());
							fact.setWarehouseReleaseDate(ds.getWarehouseReleaseDate());
							fact.setVendorOrderApprovalDate(order.getVendorOrderApprovalDate());
							fact.setVendorAckDate(order.getVendorAckDate());
							fact.setTMSReleaseTargetDate(ds.getTMSReleaseTargetDate());
							fact.setTMSReleaseDate(ds.getTMSReleaseDate());
							fact.setRequestMinItemExpiryDate(ds.getRequestMinItemExpiryDate());
							fact.setRequestIncoDateStartDate(ds.getRequestIncoDateStartDate());
							fact.setRequestIncoDateEndDate(ds.getRequestIncoDateEndDate());
							fact.setPromiseMinItemExpiryDate(ds.getPromiseMinItemExpiryDate());
							fact.setPromiseExpiryDate(dsMdfs.getPromiseExpiryDate());
							fact.setAgreedDeliveryDate(ds.getAgreedDeliveryDate());
							fact.setAgreedIncoDateEndDate(ds.getAgreedIncoDateEndDate());
							fact.setActualReceiptDate(ds.getActualReceiptDate());
							fact.setAgreedIncoDateStartDate(ds.getAgreedIncoDateStartDate());
							fact.setAgreedMinItemExpiryDate(ds.getAgreedMinItemExpiryDate());
							fact.setActualDeliveryDate(ds.getActualDeliveryDate());
							fact.setAgreedShipDate(ds.getAgreedShipDate());
							fact.setPromiseShipDate(ds.getPromiseShipDate());
							fact.setRequestShipDate(ds.getRequestShipDate());
							if (!FieldUtil.isNull(ds.getAgreedDeliveryDate())) {
								fact.setAgreedDeliveryDateLocal(getLocalFormatForDate(ds.getAgreedDeliveryDate()));
							} else {
								fact.setAgreedDeliveryDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getPromiseDeliveryDate())) {
								fact.setPromiseDeliveryDateLocal(getLocalFormatForDate(ds.getPromiseDeliveryDate()));
							} else {
								fact.setPromiseDeliveryDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getRequestDeliveryDate())) {
								fact.setRequestDeliveryDateLocal(getLocalFormatForDate(ds.getRequestDeliveryDate()));
							} else {
								fact.setRequestDeliveryDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getAgreedDeliveryDate())) {
								fact.setDeliveryDateLocal(getLocalFormatForDate(ds.getAgreedDeliveryDate()));
							} else if (!FieldUtil.isNull(ds.getPromiseDeliveryDate())) {
								fact.setDeliveryDateLocal(getLocalFormatForDate(ds.getPromiseDeliveryDate()));
							} else if (!FieldUtil.isNull(ds.getRequestDeliveryDate())) {
								fact.setDeliveryDateLocal(getLocalFormatForDate(ds.getRequestDeliveryDate()));
							} else {
								fact.setDeliveryDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getAgreedShipDate())) {
								fact.setAgreedShipDateLocal(getLocalFormatForDate(ds.getAgreedShipDate()));
							} else {
								fact.setAgreedShipDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getPromiseShipDate())) {
								fact.setPromiseShipDateLocal(getLocalFormatForDate(ds.getPromiseShipDate()));
							} else {
								fact.setPromiseShipDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getRequestShipDate())) {
								fact.setRequestShipDateLocal(getLocalFormatForDate(ds.getRequestShipDate()));
							} else {
								fact.setRequestShipDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getAgreedShipDate())) {
								fact.setShipDateLocal(getLocalFormatForDate(ds.getAgreedShipDate()));
							} else if (!FieldUtil.isNull(ds.getPromiseShipDate())) {
								fact.setShipDateLocal(getLocalFormatForDate(ds.getPromiseShipDate()));
							} else if (!FieldUtil.isNull(ds.getRequestShipDate())) {
								fact.setShipDateLocal(getLocalFormatForDate(ds.getRequestShipDate()));
							} else {
								fact.setShipDateLocal(0);
							}

							if (!FieldUtil.isNull(order.getCreationDate())) {
								fact.setCreationDateLocal(getLocalFormatForDate(order.getCreationDate()));
							} else {
								fact.setCreationDateLocal(0);
							}
							// State feilds
							fact.setDSState(ds.getState());
							fact.setOrderState(order.getState());
							fact.setLineState(line.getState());
							fact.setRSState(rs.getState());
							// Quantity fields
							if (ds.isSetOverriddenAgreedQuantity())
								fact.setOverriddenAgreedQuantity(ds.getOverriddenAgreedQuantity());
							else
								fact.unsetOverriddenAgreedQuantity();
							if (ds.isSetAgreedQuantity())
								fact.setAgreedQty(ds.getAgreedQuantity());
							else
								fact.unsetAgreedQty();
							if (ds.isSetPromiseQuantity())
								fact.setPromiseQty(ds.getPromiseQuantity());
							else
								fact.unsetPromiseQty();
							if (ds.isSetRequestQuantity())
								fact.setRequestQty(ds.getRequestQuantity());
							else
								fact.unsetRequestQty();
							if (ds.isSetPlannedShipQuantity())
								fact.setPlannedShippedQty(ds.getPlannedShipQuantity());
							else
								fact.unsetPlannedShippedQty();
							if (ds.isSetShippedQuantity())
								fact.setShippedQty(ds.getShippedQuantity());
							else
								fact.unsetShippedQty();
							if (ds.isSetReceivedQuantity())
								fact.setReceivedQty(ds.getReceivedQuantity());
							else
								fact.unsetReceivedQty();
							if (ds.isSetInvoicedQuantity())
								fact.setInvoicedQty(ds.getInvoicedQuantity());
							else
								fact.unsetInvoicedQty();
							if (ds.isSetConsignmentQuantity())
								fact.setConsignmentQty(ds.getConsignmentQuantity());
							else
								fact.unsetConsignmentQty();
							if (ds.isSetConsignmentConsumedQuantity())
								fact.setConsigmentConsumedQty(ds.getConsignmentConsumedQuantity());
							else
								fact.unsetConsigmentConsumedQty();
							if (ds.isSetBackOrderQuantity())
								fact.setBackorderedQty(ds.getBackOrderQuantity());
							else
								fact.unsetBackorderedQty();
							if (ds.isSetCancelledQuantity())
								fact.setCancelledQty(ds.getCancelledQuantity());
							else
								fact.unsetCancelledQty();
							if (ds.isSetOrigPromisedQuantity())
								fact.setOrigPromisedQuantity(ds.getOrigPromisedQuantity());
							else
								fact.unsetOrigPromisedQuantity();
							if (!FieldUtil.isNull(order.getQuantityUom()))
								fact.setQuantityUOM(order.getQuantityUom());
							else
								fact.setQuantityUOM(DvceConstants.NULL_STRING_VALUE);
							if (!FieldUtil.isNull(line.getQuantityUOM()))
								fact.setLnQuantityUOM(line.getQuantityUOM());
							else
								fact.setLnQuantityUOM(DvceConstants.NULL_STRING_VALUE);
							if (dsMdfs.isSetNettedOrderedQty())
								fact.setNettedOrderedQty(dsMdfs.getNettedOrderedQty());
							else
								fact.unsetNettedOrderedQty();
							if (dsMdfs.isSetNettedReceivedQty())
								fact.setNettedReceivedQty(dsMdfs.getNettedReceivedQty());
							else
								fact.unsetNettedReceivedQty();
							// Amount
							if (ds.isSetFreightCost())
								fact.setFreightCost(ds.getFreightCost());
							else
								fact.unsetFreightCost();
							if (ds.isSetInvoiceAmount())
								fact.setInvoiceAmount(ds.getInvoiceAmount());
							else
								fact.unsetInvoiceAmount();
							if (ds.isSetInsuranceAmount())
								fact.setInsuranceAmount(ds.getInsuranceAmount());
							else
								fact.unsetInsuranceAmount();
							if (ds.isSetRequestUnitPriceAmount())
								fact.setRequestUnitPriceAmount(ds.getRequestUnitPriceAmount());
							else
								fact.unsetRequestUnitPriceAmount();
							if (ds.isSetPromiseUnitPriceAmount())
								fact.setPromiseUnitPriceAmount(ds.getPromiseUnitPriceAmount());
							else
								fact.unsetPromiseUnitPriceAmount();
							if (ds.isSetAgreedUnitPriceAmount())
								fact.setAgreedUnitPriceAmount(ds.getAgreedUnitPriceAmount());
							else
								fact.unsetAgreedUnitPriceAmount();
							if (ds.isSetOtherCostAmount())
								fact.setOtherCostAmount(ds.getOtherCostAmount());
							else
								fact.unsetOtherCostAmount();
							if (ds.isSetCustomsDutyAmount())
								fact.setCustomDutyAmount(ds.getCustomsDutyAmount());
							else
								fact.unsetCustomDutyAmount();
							if (ds.isSetTaxAmount())
								fact.setTaxAmount(ds.getTaxAmount());
							else
								fact.unsetTaxAmount();
							if (dsMdfs.isSetConvAgreedQtyAmount())
								fact.setConvAgreedQtyAmount(dsMdfs.getConvAgreedQtyAmount());
							else
								fact.unsetConvAgreedQtyAmount();
							fact.setConvAgreedQtyUOM(dsMdfs.getConvAgreedQtyUOM());
							if (!FieldUtil.isNull(order.getTotalWeight()))
								fact.setTotalWeight(order.getTotalWeight());
							else
								fact.unsetTotalWeight();
							fact.setWeightUOM(order.getWeightUom());
							if (!FieldUtil.isNull(order.getTotalVolume()))
								fact.setTotalVolume(order.getTotalVolume());
							else
								fact.unsetTotalVolume();
							fact.setVolumeUOM(order.getVolumeUom());
							if (!FieldUtil.isNull(order.getTotalScaleUpQuantity()))
								fact.setTotalScaleUpQuantity(order.getTotalScaleUpQuantity());
							else
								fact.unsetTotalScaleUpQuantity();
							if (!FieldUtil.isNull(order.getTotalScaleUpVolume()))
								fact.setTotalScaleUpVolume(order.getTotalScaleUpVolume());
							else
								fact.unsetTotalScaleUpVolume();
							if (!FieldUtil.isNull(order.getTotalScaleUpWeight()))
								fact.setTotalScaleUpWeight(order.getTotalScaleUpWeight());
							else
								fact.unsetTotalScaleUpWeight();
							if (!FieldUtil.isNull(order.getTotalInvoiceAmount()))
								fact.setTotalInvoiceAmount(order.getTotalInvoiceAmount());
							else
								fact.unsetTotalInvoiceAmount();
							fact.setInvoiceCurrency(ds.getInvoiceCurrency());
							fact.setCurrency(line.getCurrency());
							fact.setPromiseStatus(order.getPromiseStatus());
							fact.setRSPromiseStatus(rs.getRsPromiseStatus());
							fact.setDSPromiseStatus(ds.getDsPromiseStatus());
							if (!FieldUtil.isNull(order.getSysRequisitionId()))
								fact.setRequisitionId(order.getSysRequisitionId());
							else
								fact.unsetRequisitionId();
							if (!FieldUtil.isNull(line.getSysLnProgramId())) {
								fact.setLnProgramId(line.getSysLnProgramId());
							} else {
								fact.unsetLnProgramId();
							}

							fact.setIsConsignment(order.isIsConsignment());
							fact.setIsSpot(order.isIsSpot());
							fact.setIsEmergency(order.isIsEmergency());
							fact.setIsVMI(order.isIsVMI());
							fact.setOrigin(order.getOrigin());
							if (!FieldUtil.isNull(orderMdfs.getSysContractId()))
								fact.setContractId(orderMdfs.getSysContractId());
							else
								fact.unsetContractId();
							fact.setContractNumber(orderMdfs.getContractNumber());
							if (!FieldUtil.isNull(orderMdfs.getSysContractTermsId()))
								fact.setContractTermId(orderMdfs.getSysContractTermsId());
							else
								fact.unsetContractTermId();
							fact.setContractTermNumber(orderMdfs.getContractTermsNumber());
							if (!FieldUtil.isNull(lineMdfs.getSysContractLineId()))
								fact.setContractLineId(lineMdfs.getSysContractLineId());
							else
								fact.unsetContractLineId();
							fact.setContractLineNumber(lineMdfs.getContractLineLineNumber());
							fact.setIsExpedite(order.isIsExpedite());
							if (!FieldUtil.isNull(order.getSysParentOrderId()))
								fact.setParentOrderId(order.getSysParentOrderId());
							else
								fact.unsetParentOrderId();
							fact.setParentOrderNumber(order.getParentOrderOrderNumber());
							fact.setExtParentOrderNumber(order.getExtParentOrderNumber());
							fact.setOMSLnCancelCollabStatus(lineMdfs.getLnCancelCollaborationStatus());
							fact.setCancelCollabStatus(orderMdfs.getCancelCollaborationStatus());
							fact.setRsCancelCollabStatus(rsMdfs.getRsCancelCollaborationStatus());
							if (!FieldUtil.isNull(ds.getSysPromiseItemId()))
								fact.setPromiseItemId(ds.getSysPromiseItemId());
							else
								fact.unsetPromiseItemId();
							ItemRow promiseItemRow = null;
							if (!FieldUtil.isNull(ds.getSysPromiseItemId())) {
								promiseItemRow = ItemCacheManager.getInstance().getItem(ds.getSysPromiseItemId());
								fact.setPromiseItemDescription(promiseItemRow.getDescription());
								fact.setPromiseItemName(promiseItemRow.getItemName());
								fact.setPromiseItemEntName(promiseItemRow.getEntName());
							} else {
								fact.setPromiseItemDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setPromiseItemName(DvceConstants.NULL_STRING_VALUE);
								fact.setPromiseItemEntName(DvceConstants.NULL_STRING_VALUE);
							}
							fact.setShipWithGrpRef(ds.getShipWithGroupRef());
							fact.setExtShipToSiteName(rs.getExtShipToSiteName());
							fact.setExtShipToLocationName(rs.getExtShipToLocationName());
							fact.setExtShipFromSiteName(ds.getExtShipFromSiteName());
							fact.setExtShipFromLocationName(ds.getExtShipFromLocationName());
							if (!FieldUtil.isNull(rs.getSysShipToSiteId()))
								fact.setPartnerSiteName(
										OMSUtil.getPartnerSiteName(rs.getSysShipToSiteId(), vcDvceContext));
							else
								fact.setPartnerSiteName(DvceConstants.NULL_STRING_VALUE);
							fact.setOrderClassification(order.getOrderClassification());
							fact.setLevelModifiedDate(ds.getLevelModifiedDate());
							fact.setBPONumber(order.getBPONumber());
							fact.setBPOLineNumber(line.getBPOLineNumber());
							if (!FieldUtil.isNull(order.getSysProgramId()))
								fact.setProgramId(order.getSysProgramId());
							else
								fact.unsetProgramId();
							fact.setProgramName(order.getProgramName());
							fact.setProgramEntName(order.getProgramEnterpriseName());
							if (lineMdfs.isSetPricePer())
								fact.setPricePer(lineMdfs.getPricePer());
							else
								fact.unsetPricePer();
							if (line.isSetLineAmount())
								fact.setLineAmount(line.getLineAmount());
							else
								fact.unsetLineAmount();
							fact.setExternalDocNumber(ds.getExternalDocNumber());
							fact.setOverrideReasonCode(ds.getOverrideReasonCode());
							fact.setExtFreightForwarderName(order.getExtFreightForwarderName());
							fact.setBuyerCode(order.getBuyerCode());
							fact.setPlannerCode(order.getPlannerCode());
							fact.setInternational(order.isInternational());
							fact.setIsDutyFree(line.isIsDutyFree());
							fact.setRSIsExpedite(rs.isRsIsExpedite());
							if (dsMdfs.isSetNettedOrderedQty())
								fact.setNettedOrderedQty(dsMdfs.getNettedOrderedQty());
							else
								fact.unsetNettedOrderedQty();
							if (dsMdfs.isSetNettedReceivedQty())
								fact.setNettedReceivedQty(dsMdfs.getNettedReceivedQty());
							else
								fact.unsetNettedReceivedQty();
							fact.setFOBCode(orderMdfs.getFOBCode());
							if (pglrRow != null) {
								fact.setProductGroupLevelId(line.getSysProductGroupLevelId());
								fact.setProductGroupLevel1Name(pglrRow.getLevel1Name());
								fact.setProductGroupLevel2Name(pglrRow.getLevel2Name());
								fact.setProductGroupLevel3Name(pglrRow.getLevel3Name());
								fact.setProductGroupLevel4Name(pglrRow.getLevel4Name());
								fact.setProductGroupLevel5Name(pglrRow.getLevel5Name());
								fact.setProductGroupLevelEntName(pglrRow.getEntName());
								fact.setProductGroupTypeName(pglrRow.getTypeName());
								fact.setLevelNo(pglrRow.getLevelNo());
								fact.setLevelName(pglrRow.getLevelName());
								fact.setLevelDesc(pglrRow.getLevelDesc());
							} else {
								fact.unsetProductGroupLevelId();
								fact.setProductGroupLevel1Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevel2Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevel3Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevel4Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevel5Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevelEntName(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupTypeName(DvceConstants.NULL_STRING_VALUE);
								fact.unsetLevelNo();
								fact.setLevelName(DvceConstants.NULL_STRING_VALUE);
								fact.setLevelDesc(DvceConstants.NULL_STRING_VALUE);
							}

							fact.setExtEndCustomerPONo(dsMdfs.getExtEndCustomerPONo());
							fact.setFOBPoint(orderMdfs.getFOBPoint());
							fact.setIsHazardous(dsMdfs.isIsHazardous());
							fact.setPaymentTermsCode(orderMdfs.getPaymentTermsCode());
							if (!FieldUtil.isNull(orderMdfs.getSysPaymentTermsId()))
								fact.setPaymentTermsId(orderMdfs.getSysPaymentTermsId());
							else
								fact.unsetPaymentTermsId();
							fact.setPaymentTermsEnterpriseName(orderMdfs.getPaymentTermsEnterpriseName());
							fact.setDeviationReasonCode(dsMdfs.getDeviationReasonCode());
              fact.setVendorDeviationComment(dsMdfs.getVendorDeviationComment());
              fact.setBuyerCollabReasonCode(dsMdfs.getBuyerCollabReasonCode());
              fact.setBuyerCollabReasonComment(dsMdfs.getBuyerCollabReasonComment());
							if (dsMdfs.isSetPromisePricePer())
								fact.setPromisePricePer(dsMdfs.getPromisePricePer());
							else
								fact.unsetPromisePricePer();
							if (dsMdfs.isSetRequestPricePer())
								fact.setRequestPricePer(dsMdfs.getRequestPricePer());
							else
								fact.unsetRequestPricePer();
							if (dsMdfs.isSetAgreedPricePer())
								fact.setAgreedPricePer(dsMdfs.getAgreedPricePer());
							else
								fact.unsetAgreedPricePer();
							if (dsMdfs.isSetTotalCost())
								fact.setTotalCost(dsMdfs.getTotalCost());
							else
								fact.unsetTotalCost();
							fact.setBackOrderReasonCode(dsMdfs.getBackOrderReasonCode());
							fact.setAutoMoveToReceived(orderMdfs.isAutoMoveToReceived());
							fact.setIncoTermsLocation(orderMdfs.getIncoTermsLocation());
							fact.setASNCreationDate(ds.getASNCreationDate());
							fact.setReceivingWhseStatus(ds.getReceivingWhseStatus());
							fact.setShippingWhseStatus(ds.getShippingWhseStatus());
							fact.setShipmentStatus(ds.getShipmentStatus());
							if (!FieldUtil.isNull(rs.getSysShipToLocationId()))
								fact.setShipToLocationId(rs.getSysShipToLocationId());
							else
								fact.unsetShipToLocationId();
							fact.setShipToLocationName(rs.getShipToLocationName());
							fact.setShipToLocationAddress(rs.getShipToLocationAddress());
							fact.setShipToLocationSiteName(rs.getShipToLocationSiteName());
							if (!FieldUtil.isNull(ds.getSysShipFromLocationId()))
								fact.setShipFromLocationId(ds.getSysShipFromLocationId());
							else
								fact.unsetShipFromLocationId();
							fact.setShipFromLocationName(ds.getShipFromLocationName());
							fact.setShipFromLocationAddress(ds.getShipFromLocationAddress());
							fact.setShipFromLocationSiteName(ds.getShipFromLocationSiteName());
							fact.setExtShipToLocationName(rs.getExtShipToLocationName());
							fact.setExtShipFromLocationName(ds.getExtShipFromLocationName());
							SiteRow shipFromSite = null;
							if (!FieldUtil.isNull(ds.getSysShipFromSiteId())) {
								shipFromSite = SiteCacheManager.getInstance().getSite(ds.getSysShipFromSiteId(),
										vcDvceContext);
							}
							if (shipToSite != null) {
								fact.setShipToSiteId(shipToSite.getSysSiteId());
								fact.setShipToSiteName(shipToSite.getSiteName());
								fact.setShipToDescription(shipToSite.getDescription());
								fact.setShipToTimeZoneId(shipToSite.getTimeZoneId());
							} else {
								fact.unsetShipToSiteId();
								fact.setShipToSiteName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToTimeZoneId(DvceConstants.NULL_STRING_VALUE);
							}
							if (shipFromSite != null) {
								fact.setShipFromSiteId(shipFromSite.getSysSiteId());
								fact.setShipFromSiteName(shipFromSite.getSiteName());
								fact.setShipFromDescription(shipFromSite.getDescription());
								fact.setShipFromTimeZoneId(shipFromSite.getTimeZoneId());
							} else {
								fact.unsetShipFromSiteId();
								fact.setShipFromSiteName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromTimeZoneId(DvceConstants.NULL_STRING_VALUE);
							}
							fact.setExtBPONumber(fact.getExtBPONumber());
							fact.setLnCurrency(line.getCurrency());
							if (order.isSetTotalQuantity())
								fact.setTotalQuantity(order.getTotalQuantity());
							else
								fact.unsetTotalQuantity();
							if (rs.isSetDemandQuantity())
								fact.setDemandQty(rs.getDemandQuantity());
							else
								fact.unsetDemandQty();
							if (rs.isSetOrigRequestQuantity())
								fact.setOrigRequestQty(rs.getOrigRequestQuantity());
							else
								fact.unsetOrigRequestQty();
							fact.setOverrideComment(ds.getOverrideComment());
							if (rs.isSetDemandScaleUpQuantity())
								fact.setDemandScaleUpQuantity(rs.getDemandScaleUpQuantity());
							else
								fact.unsetDemandScaleUpQuantity();
							if (ds.isSetReturnedQuantity())
								fact.setReturnedQuantity(ds.getReturnedQuantity());
							else
								fact.unsetReturnedQuantity();
							if (ds.isSetRejectedQuantity())
								fact.setRejectedQuantity(ds.getRejectedQuantity());
							else
								fact.unsetRejectedQuantity();
							if (rs.isSetProjectedDaysOfSupply())
								fact.setProjectedDaysOfSupply(rs.getProjectedDaysOfSupply());
							else
								fact.unsetProjectedDaysOfSupply();
							if (order.isSetTotalAmount())
								fact.setTotalAmount(order.getTotalAmount());
							else
								fact.unsetTotalAmount();
							fact.setExtVendorItemName(line.getExtVendorItemName());
				              SiteRow consolidationSite = null;
				              if(!FieldUtil.isNull(ds.getSysConsolidationSiteId())) {
				                consolidationSite = SiteCacheManager.getInstance().getSite(ds.getSysConsolidationSiteId(), context);
				                if(consolidationSite != null) {
				                  fact.setConsolidationSiteId(consolidationSite.getSysSiteId());
				                  fact.setConsolidationSiteName(consolidationSite.getSiteName());
				                  fact.setConsolidationSiteDescription(consolidationSite.getDescription());
				                  fact.setConsolidationSiteTimeZoneId(consolidationSite.getTimeZoneId());
				                } else {
				                  fact.unsetConsolidationSiteId();
				                  fact.setConsolidationSiteName(DvceConstants.NULL_STRING_VALUE);
				                  fact.setConsolidationSiteDescription(DvceConstants.NULL_STRING_VALUE);
				                  fact.setConsolidationSiteTimeZoneId(DvceConstants.NULL_STRING_VALUE);
				                }
				              }
				              fact.setConsolidationDeliveryDate(ds.getConsolidationDeliveryDate());
				              fact.setConsolidationPickupDate(ds.getConsolidationPickupDate());
				              poOrderFact.add(fact);
						} else if (OrderTypeEnum.SALES_ORDER.stringValue().equals(order.getOrderType())) {
							SalesOrderFact fact = new SalesOrderFact();
							fact.setOrderId(order.getSysId());
							fact.setLineId(line.getSysId());
							fact.setRequestScheduleId(rs.getSysId());
							fact.setDeliveryScheduleId(ds.getSysId());
							fact.setOrderNumber(order.getOrderNumber());
							fact.setLineNumber(line.getLineNumber());
							fact.setRequestScheduleNumber(rs.getRequestScheduleNumber());
							fact.setDeliveryScheduleNumber(ds.getDeliveryScheduleNumber());
							if (!FieldUtil.isNull(line.getSpecificItemName()))
								fact.setSpecificItemName(line.getSpecificItemName());
							else
								fact.setSpecificItemName(DvceConstants.NULL_STRING_VALUE);
							if (!FieldUtil.isNull(line.getGenericItemName()))
								fact.setGenericItemName(line.getGenericItemName());
							else
								fact.setGenericItemName(DvceConstants.NULL_STRING_VALUE);
							if (!FieldUtil.isNull(ds.getSysShipFromSiteId())) {
								fact.setShipFromSiteId(ds.getSysShipFromSiteId());
								SiteRow shipFromSiteRow = SiteCacheManager.getInstance()
										.getSite(ds.getSysShipFromSiteId(), vcDvceContext);
								fact.setShipFromSiteName(shipFromSiteRow.getSiteName());
								fact.setShipFromDescription(shipFromSiteRow.getDescription());
								fact.setShipFromIsDC(shipFromSiteRow.isIsDc());
								fact.setShipFromIsPlant(shipFromSiteRow.isIsPlant());
							} else {
								fact.unsetShipFromSiteId();
								fact.setShipFromSiteName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromDescription(DvceConstants.NULL_STRING_VALUE);
								fact.unsetShipFromIsDC();
								fact.unsetShipFromIsPlant();
							}
							if (!FieldUtil.isNull(rs.getSysShipToSiteId())) {
								SiteRow shipToSiteRow = SiteCacheManager.getInstance().getSite(rs.getSysShipToSiteId(),
										vcDvceContext);
								fact.setShipToSiteId(rs.getSysShipToSiteId());
								fact.setShipToDescription(shipToSiteRow.getDescription());
								fact.setShipToSiteName(shipToSiteRow.getSiteName());
								fact.setShiptoSiteIsPlant(shipToSiteRow.isIsDc());
								fact.setShiptoSiteIsDC(shipToSiteRow.isIsPlant());
							} else {
								fact.unsetShipToSiteId();
								fact.setShipToDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToSiteName(DvceConstants.NULL_STRING_VALUE);
								fact.unsetShiptoSiteIsPlant();
								fact.unsetShiptoSiteIsDC();
							}
							if (buyingOrg != null) {
								fact.setBuyingOrgId(order.getSysBuyingOrgId());
								fact.setBuyingOrgDescription(buyingOrg.getDescription());
								fact.setBuyingOrgName(buyingOrg.getOrgName());
								fact.setBuyingOrgEntName(buyingOrg.getEntName());
							} else {
								fact.unsetBuyingOrgId();
								fact.setBuyingOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setBuyingOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setBuyingOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (sellingOrg != null) {
								fact.setSellingOrgId(order.getSysSellingOrgId());
								fact.setSellingOrgDescription(sellingOrg.getDescription());
								fact.setSellingOrgName(sellingOrg.getOrgName());
								fact.setSellingOrgEntName(sellingOrg.getEntName());
							} else {
								fact.unsetSellingOrgId();
								fact.setSellingOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setSellingOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setSellingOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (owningOrg != null) {
								fact.setOwningOrgId(order.getSysOwningOrgId());
								fact.setOwningOrgDescription(owningOrg.getDescription());
								fact.setOwningOrgName(owningOrg.getOrgName());
								fact.setOwningOrgEntName(owningOrg.getEntName());
							} else {
								fact.unsetOwningOrgId();
								fact.setOwningOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setOwningOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setOwningOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (custOfBuyerOrg != null) {
								fact.setCustomerOfBuyerOrgId(order.getSysCustOfBuyerOrgId());
								fact.setCustomerOfBuyerDescription(custOfBuyerOrg.getDescription());
								fact.setCustOfBuyerOrgName(custOfBuyerOrg.getOrgName());
								fact.setCustOfBuyerOrgEntName(custOfBuyerOrg.getEntName());
							} else {
								fact.unsetCustomerOfBuyerOrgId();
								fact.setCustomerOfBuyerDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setCustOfBuyerOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setCustOfBuyerOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (omoOrg != null) {
								fact.setOMOOrgId(order.getSysOrderMgmtOrgId());
								fact.setOMOOrgDescription(omoOrg.getDescription());
								fact.setOMOOrgName(omoOrg.getOrgName());
								fact.setOMOOrgEntName(omoOrg.getEntName());
							} else {
								fact.unsetOMOOrgId();
								fact.setOMOOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setOMOOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setOMOOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (fulFillmentOrg != null) {
								fact.setFulfillmentOrgId(order.getSysFulfillmentOrgId());
								fact.setFulfillmentOrgDescription(fulFillmentOrg.getDescription());
								fact.setFulfillmentOrgName(fulFillmentOrg.getOrgName());
								fact.setFulfillmentOrgEntName(fulFillmentOrg.getEntName());
							} else {
								fact.unsetFulfillmentOrgId();
								fact.setFulfillmentOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setFulfillmentOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setFulfillmentOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (freightFwdOrg != null) {
								fact.setFreightFwdOrgId(order.getSysFreightFwdOrgId());
								fact.setFreightFwdOrgDescription(freightFwdOrg.getDescription());
								fact.setFreightFwdOrgName(freightFwdOrg.getOrgName());
								fact.setFreightFwdOrgEntName(freightFwdOrg.getEntName());
							} else {
								fact.unsetFreightFwdOrgId();
								fact.setFreightFwdOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setFreightFwdOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setFreightFwdOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (shipFromOrg != null) {
								fact.setShipFromOrgId(order.getSysShipFromOrgId());
								fact.setShipFromOrgDescription(shipFromOrg.getDescription());
								fact.setShipFromOrgName(shipFromOrg.getOrgName());
								fact.setShipFromOrgEntName(shipFromOrg.getEntName());
							} else {
								fact.unsetShipFromOrgId();
								fact.setShipFromOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (shipToOrg != null) {
								fact.setShipToOrgId(order.getSysShipToOrgId());
								fact.setShipToOrgDescription(shipToOrg.getDescription());
								fact.setShipToOrgName(shipToOrg.getOrgName());
								fact.setShipToOrgEntName(shipToOrg.getEntName());
							} else {
								fact.unsetShipToOrgId();
								fact.setShipToOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}

              if (!FieldUtil.isNull(rs.getSysRsShipToOrgId())) {
							fact.setRsShipToOrgId(rs.getSysRsShipToOrgId());
              } else {
                fact.setRsShipToOrgId(DvceConstants.NULL_LONG_VALUE);
              }
              if (!FieldUtil.isNull(rs.getRsShipToOrgName())) {
							fact.setRsShipToOrgName(rs.getRsShipToOrgName());
              } else {
                fact.setRsShipToOrgName(DvceConstants.NULL_STRING_VALUE);
              }
              if (!FieldUtil.isNull(rs.getShipToAddress()))
                 fact.setShipToAddress(rs.getShipToAddress());
              else {
                fact.setShipToAddress(DvceConstants.NULL_ADDRESS_VALUE);
              }                 
              if (!FieldUtil.isNull(ds.getShipFromAddress()))
                 fact.setShipFromAddress(ds.getShipFromAddress());
              else {
                 fact.setShipFromAddress(DvceConstants.NULL_ADDRESS_VALUE);
              }  
							if (tcoOrg != null) {
								fact.setTCOOrgId(order.getSysTCOOrgId());
								fact.setTCOrgDescription(tcoOrg.getDescription());
								fact.setTCOrgName(tcoOrg.getOrgName());
								fact.setTCOrgEntName(tcoOrg.getEntName());
							} else {
								fact.unsetTCOOrgId();
								fact.setTCOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setTCOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setTCOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (itemRow != null) {
								fact.setItemId(line.getSysItemId());
								fact.setItemDescription(itemRow.getDescription());
								fact.setItemName(itemRow.getItemName());
								fact.setItemEnterpriseName(itemRow.getEntName());
								fact.setExtMfgItemName(itemRow.getExtMfgItemName());
							} else {
								fact.unsetItemId();
								fact.setItemDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setItemName(DvceConstants.NULL_STRING_VALUE);
								fact.setItemEnterpriseName(DvceConstants.NULL_STRING_VALUE);
								fact.setExtMfgItemName(DvceConstants.NULL_STRING_VALUE);
							}
							if (partnerRow != null) {
								fact.setVendorId(order.getSysVendorId());
								fact.setVendorDescription(partnerRow.getDescription());
								fact.setVendorName(partnerRow.getPartnerName());
								fact.setVendorOrgName(partnerRow.getPartnerOrgName());
							} else {
								fact.unsetVendorId();
								fact.setVendorDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setVendorName(DvceConstants.NULL_STRING_VALUE);
								fact.setVendorName(DvceConstants.NULL_STRING_VALUE);
							}
							fact.setRemitToAddress(order.getRemitToAddress());
							fact.setBillToAddress(order.getBillToAddress());
							fact.setLineType(line.getLineType());
							if (!FieldUtil.isNull(line.getUnitPrice()))
								fact.setUnitPrice(line.getUnitPrice());
							else
								fact.unsetUnitPrice();
							fact.setSellingAgent1(order.getSellingAgent1Name());
							fact.setSellingAgent2(order.getSellingAgent2Name());
							fact.setSellingAgent3(order.getSellingAgent3Name());
							fact.setSellingAgent4(order.getSellingAgent4Name());
							fact.setBuyingAgent1(order.getBuyingAgent1Name());
							fact.setBuyingAgent2(order.getBuyingAgent2Name());
							fact.setBuyingAgent3(order.getBuyingAgent3Name());
							fact.setBuyingAgent4(order.getBuyingAgent4Name());
							fact.setSellingAgent1EntName(order.getSellingAgent1EnterpriseName());
							fact.setSellingAgent2EntName(order.getSellingAgent2EnterpriseName());
							fact.setSellingAgent3EntName(order.getSellingAgent3EnterpriseName());
							fact.setSellingAgent4EntName(order.getSellingAgent4EnterpriseName());
							fact.setBuyingAgent1EntName(order.getBuyingAgent1EnterpriseName());
							fact.setBuyingAgent2EntName(order.getBuyingAgent2EnterpriseName());
							fact.setBuyingAgent3EntName(order.getBuyingAgent3EnterpriseName());
							fact.setBuyingAgent4EntName(order.getBuyingAgent4EnterpriseName());
							fact.setIsPendingAuthorization(orderMdfs.isIsPendingAuthorization());
							if (!FieldUtil.isNull(line.getSysVendorItemId()))
								fact.setVendorItemId(line.getSysVendorItemId());
							else
								fact.unsetVendorItemId();
							if (!FieldUtil.isNull(line.getSysGenericItemId()))
								fact.setGenericItemId(line.getSysGenericItemId());
							else
								fact.unsetGenericItemId();
							if (!FieldUtil.isNull(line.getSysSpecificItemId()))
								fact.setSpecificItemId(line.getSysSpecificItemId());
							else
								fact.unsetSpecificItemId();
							if (!FieldUtil.isNull(order.getSysSellingAgent1Id()))
								fact.setSellingAgent1Id(order.getSysSellingAgent1Id());
							else
								fact.unsetSellingAgent1Id();
							if (!FieldUtil.isNull(order.getSysSellingAgent2Id()))
								fact.setSellingAgent2Id(order.getSysSellingAgent2Id());
							else
								fact.unsetSellingAgent2Id();
							if (!FieldUtil.isNull(order.getSysSellingAgent3Id()))
								fact.setSellingAgent3Id(order.getSysSellingAgent3Id());
							else
								fact.unsetSellingAgent3Id();
							if (!FieldUtil.isNull(order.getSysSellingAgent4Id()))
								fact.setSellingAgent4Id(order.getSysSellingAgent4Id());
							else
								fact.unsetSellingAgent4Id();
							if (!FieldUtil.isNull(order.getSysBuyingAgent1Id()))
								fact.setBuyingAgent1Id(order.getSysBuyingAgent1Id());
							else
								fact.unsetBuyingAgent1Id();
							if (!FieldUtil.isNull(order.getSysBuyingAgent2Id()))
								fact.setBuyingAgent2Id(order.getSysBuyingAgent2Id());
							else
								fact.unsetBuyingAgent2Id();
							if (!FieldUtil.isNull(order.getSysBuyingAgent3Id()))
								fact.setBuyingAgent3Id(order.getSysBuyingAgent3Id());
							else
								fact.unsetBuyingAgent3Id();
							if (!FieldUtil.isNull(order.getSysBuyingAgent4Id()))
								fact.setBuyingAgent4Id(order.getSysBuyingAgent4Id());
							else
								fact.unsetBuyingAgent4Id();
							if (!FieldUtil.isNull(ds.getSysDsShipFromOrgId()))
								fact.setDsShipFromOrgId(ds.getSysDsShipFromOrgId());
							else
								fact.unsetDsShipFromOrgId();
							DeliveryScheduleMDFs dsMdfs = ds.getMDFs(DeliveryScheduleMDFs.class);
							if (!FieldUtil.isNull(order.getSysClonedFromOrderId()))
								fact.setClonedFromOrderId(order.getSysClonedFromOrderId());
							else
								fact.unsetClonedFromOrderId();
							fact.setOrderSubType(order.getOrderSubType());
							fact.setVendorRejectReasonCode(ds.getVendorRejectReasonCode());
							fact.setOrigRequestQtyUOM(rs.getOriginalRequestQuantityUOM());
							fact.setSalesOrderNumber(order.getSalesOrderNumber());
							fact.setPurchaseOrderNumber(order.getPurchaseOrderNumber());
							fact.setOMOOrderNumber(order.getOmoOrderNumber());
							fact.setWMSOrderNumber(order.getWmsOrderNumber());
							fact.setSubmitForApprovalDate(order.getSubmitForApprovalDate());
							fact.setDeliveryGroupNumber(ds.getDeliveryGroupNumber());
							if (!FieldUtil.isNull(order.getSysTransModeId()))
								fact.setTransModeId(order.getSysTransModeId());
							else
								fact.unsetTransModeId();
							if (!FieldUtil.isNull(rs.getSysRsTransModeId()))
								fact.setRsTransModeId(rs.getSysRsTransModeId());
							else
								fact.unsetRsTransModeId();
							// Date fields
							if (!FieldUtil.isNull(order.getSysCreationOrgId()))
								fact.setCreationOrgId(order.getSysCreationOrgId());
							fact.setRequestDeliveryDate(ds.getRequestDeliveryDate());
							fact.setPromiseDeliveryDate(ds.getPromiseDeliveryDate());
							fact.setASNCreationDate(ds.getASNCreationDate());
							fact.setCreationDate(order.getCreationDate());
							fact.setTransMode(order.getTransModeName());
							fact.setIncoTerms(order.getIncoTerms());
							fact.setExtItemName(line.getExtItemName());
							fact.setBuyerOrderApprovalDate(order.getBuyerOrderApprovalDate());
							fact.setOrigPromiseDeliveryDate(ds.getOrigPromiseDeliveryDate());
							fact.setOrigPromiseShipDate(ds.getOrigPromiseShipDate());
							fact.setOrigRequestShipDate(rs.getOrigRequestShipDate());
							fact.setOrigRequestDeliveryDate(rs.getOrigRequestDeliveryDate());
							fact.setOverriddenDeliveryDate(ds.getOverriddenDeliveryDate());
							fact.setOverriddenShipDate(ds.getOverriddenShipDate());
							fact.setPlannedShipDate(ds.getPlannedShipDate());
							fact.setWhseReleaseTargetDate(ds.getWhseReleaseTargetDate());
							fact.setWarehouseReleaseDate(ds.getWarehouseReleaseDate());
							fact.setVendorOrderApprovalDate(order.getVendorOrderApprovalDate());
							fact.setVendorAckDate(order.getVendorAckDate());
							fact.setTMSReleaseTargetDate(ds.getTMSReleaseTargetDate());
							fact.setTMSReleaseDate(ds.getTMSReleaseDate());
							fact.setRequestMinItemExpiryDate(ds.getRequestMinItemExpiryDate());
							fact.setRequestIncoDateStartDate(ds.getRequestIncoDateStartDate());
							fact.setRequestIncoDateEndDate(ds.getRequestIncoDateEndDate());
							fact.setPromiseMinItemExpiryDate(ds.getPromiseMinItemExpiryDate());
							fact.setPromiseExpiryDate(dsMdfs.getPromiseExpiryDate());
							fact.setAgreedDeliveryDate(ds.getAgreedDeliveryDate());
							fact.setAgreedIncoDateEndDate(ds.getAgreedIncoDateEndDate());
							fact.setActualReceiptDate(ds.getActualReceiptDate());
							fact.setAgreedIncoDateStartDate(ds.getAgreedIncoDateStartDate());
							fact.setAgreedMinItemExpiryDate(ds.getAgreedMinItemExpiryDate());
							fact.setActualDeliveryDate(ds.getActualDeliveryDate());
							fact.setAgreedShipDate(ds.getAgreedShipDate());
							fact.setPromiseShipDate(ds.getPromiseShipDate());
							fact.setRequestShipDate(ds.getRequestShipDate());
							if (!FieldUtil.isNull(ds.getAgreedDeliveryDate())) {
								fact.setAgreedDeliveryDateLocal(getLocalFormatForDate(ds.getAgreedDeliveryDate()));
							} else {
								fact.setAgreedDeliveryDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getPromiseDeliveryDate())) {
								fact.setPromiseDeliveryDateLocal(getLocalFormatForDate(ds.getPromiseDeliveryDate()));
							} else {
								fact.setPromiseDeliveryDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getRequestDeliveryDate())) {
								fact.setRequestDeliveryDateLocal(getLocalFormatForDate(ds.getRequestDeliveryDate()));
							} else {
								fact.setRequestDeliveryDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getAgreedDeliveryDate())) {
								fact.setDeliveryDateLocal(getLocalFormatForDate(ds.getAgreedDeliveryDate()));
							} else if (!FieldUtil.isNull(ds.getPromiseDeliveryDate())) {
								fact.setDeliveryDateLocal(getLocalFormatForDate(ds.getPromiseDeliveryDate()));
							} else if (!FieldUtil.isNull(ds.getRequestDeliveryDate())) {
								fact.setDeliveryDateLocal(getLocalFormatForDate(ds.getRequestDeliveryDate()));
							} else {
								fact.setDeliveryDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getAgreedShipDate())) {
								fact.setAgreedShipDateLocal(getLocalFormatForDate(ds.getAgreedShipDate()));
							} else {
								fact.setAgreedShipDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getPromiseShipDate())) {
								fact.setPromiseShipDateLocal(getLocalFormatForDate(ds.getPromiseShipDate()));
							} else {
								fact.setPromiseShipDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getRequestShipDate())) {
								fact.setRequestShipDateLocal(getLocalFormatForDate(ds.getRequestShipDate()));
							} else {
								fact.setRequestShipDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getAgreedShipDate())) {
								fact.setShipDateLocal(getLocalFormatForDate(ds.getAgreedShipDate()));
							} else if (!FieldUtil.isNull(ds.getPromiseShipDate())) {
								fact.setShipDateLocal(getLocalFormatForDate(ds.getPromiseShipDate()));
							} else if (!FieldUtil.isNull(ds.getRequestShipDate())) {
								fact.setShipDateLocal(getLocalFormatForDate(ds.getRequestShipDate()));
							} else {
								fact.setShipDateLocal(0);
							}

							if (!FieldUtil.isNull(order.getCreationDate())) {
								fact.setCreationDateLocal(getLocalFormatForDate(order.getCreationDate()));
							} else {
								fact.setCreationDateLocal(0);
							}
							// State feilds
							fact.setDSState(ds.getState());
							fact.setOrderState(order.getState());
							fact.setLineState(line.getState());
							fact.setRSState(rs.getState());
							// Quantity fields
							if (ds.isSetOverriddenAgreedQuantity())
								fact.setOverriddenAgreedQuantity(ds.getOverriddenAgreedQuantity());
							else
								fact.unsetOverriddenAgreedQuantity();
							if (ds.isSetAgreedQuantity())
								fact.setAgreedQty(ds.getAgreedQuantity());
							else
								fact.unsetAgreedQty();
							if (ds.isSetPromiseQuantity())
								fact.setPromiseQty(ds.getPromiseQuantity());
							else
								fact.unsetPromiseQty();
							if (ds.isSetRequestQuantity())
								fact.setRequestQty(ds.getRequestQuantity());
							else
								fact.unsetRequestQty();
							if (ds.isSetPlannedShipQuantity())
								fact.setPlannedShippedQty(ds.getPlannedShipQuantity());
							else
								fact.unsetPlannedShippedQty();
							if (ds.isSetShippedQuantity())
								fact.setShippedQty(ds.getShippedQuantity());
							else
								fact.unsetShippedQty();
							if (ds.isSetReceivedQuantity())
								fact.setReceivedQty(ds.getReceivedQuantity());
							else
								fact.unsetReceivedQty();
							if (ds.isSetInvoicedQuantity())
								fact.setInvoicedQty(ds.getInvoicedQuantity());
							else
								fact.unsetInvoicedQty();
							if (ds.isSetConsignmentQuantity())
								fact.setConsignmentQty(ds.getConsignmentQuantity());
							else
								fact.unsetConsignmentQty();
							if (ds.isSetConsignmentConsumedQuantity())
								fact.setConsigmentConsumedQty(ds.getConsignmentConsumedQuantity());
							else
								fact.unsetConsigmentConsumedQty();
							if (ds.isSetBackOrderQuantity())
								fact.setBackorderedQty(ds.getBackOrderQuantity());
							else
								fact.unsetBackorderedQty();
							if (ds.isSetCancelledQuantity())
								fact.setCancelledQty(ds.getCancelledQuantity());
							else
								fact.unsetCancelledQty();
							if (ds.isSetOrigPromisedQuantity())
								fact.setOrigPromisedQuantity(ds.getOrigPromisedQuantity());
							else
								fact.unsetOrigPromisedQuantity();
							if (!FieldUtil.isNull(order.getQuantityUom()))
								fact.setQuantityUOM(order.getQuantityUom());
							else
								fact.setQuantityUOM(DvceConstants.NULL_STRING_VALUE);
							if (!FieldUtil.isNull(line.getQuantityUOM()))
								fact.setLnQuantityUOM(line.getQuantityUOM());
							else
								fact.setLnQuantityUOM(DvceConstants.NULL_STRING_VALUE);
							if (dsMdfs.isSetNettedOrderedQty())
								fact.setNettedOrderedQty(dsMdfs.getNettedOrderedQty());
							else
								fact.unsetNettedOrderedQty();
							if (dsMdfs.isSetNettedReceivedQty())
								fact.setNettedReceivedQty(dsMdfs.getNettedReceivedQty());
							else
								fact.unsetNettedReceivedQty();
							// Amount
							if (ds.isSetFreightCost())
								fact.setFreightCost(ds.getFreightCost());
							else
								fact.unsetFreightCost();
							if (ds.isSetInvoiceAmount())
								fact.setInvoiceAmount(ds.getInvoiceAmount());
							else
								fact.unsetInvoiceAmount();
							if (ds.isSetInsuranceAmount())
								fact.setInsuranceAmount(ds.getInsuranceAmount());
							else
								fact.unsetInsuranceAmount();
							if (ds.isSetRequestUnitPriceAmount())
								fact.setRequestUnitPriceAmount(ds.getRequestUnitPriceAmount());
							else
								fact.unsetRequestUnitPriceAmount();
							if (ds.isSetPromiseUnitPriceAmount())
								fact.setPromiseUnitPriceAmount(ds.getPromiseUnitPriceAmount());
							else
								fact.unsetPromiseUnitPriceAmount();
							if (ds.isSetAgreedUnitPriceAmount())
								fact.setAgreedUnitPriceAmount(ds.getAgreedUnitPriceAmount());
							else
								fact.unsetAgreedUnitPriceAmount();
							if (ds.isSetOtherCostAmount())
								fact.setOtherCostAmount(ds.getOtherCostAmount());
							else
								fact.unsetOtherCostAmount();
							if (ds.isSetCustomsDutyAmount())
								fact.setCustomDutyAmount(ds.getCustomsDutyAmount());
							else
								fact.unsetCustomDutyAmount();
							if (ds.isSetTaxAmount())
								fact.setTaxAmount(ds.getTaxAmount());
							else
								fact.unsetTaxAmount();
							if (dsMdfs.isSetConvAgreedQtyAmount())
								fact.setConvAgreedQtyAmount(dsMdfs.getConvAgreedQtyAmount());
							else
								fact.unsetConvAgreedQtyAmount();
							fact.setConvAgreedQtyUOM(dsMdfs.getConvAgreedQtyUOM());
							if (!FieldUtil.isNull(order.getTotalWeight()))
								fact.setTotalWeight(order.getTotalWeight());
							else
								fact.unsetTotalWeight();
							fact.setWeightUOM(order.getWeightUom());
							if (!FieldUtil.isNull(order.getTotalVolume()))
								fact.setTotalVolume(order.getTotalVolume());
							else
								fact.unsetTotalVolume();
							fact.setVolumeUOM(order.getVolumeUom());
							if (!FieldUtil.isNull(order.getTotalScaleUpQuantity()))
								fact.setTotalScaleUpQuantity(order.getTotalScaleUpQuantity());
							else
								fact.unsetTotalScaleUpQuantity();
							if (!FieldUtil.isNull(order.getTotalScaleUpVolume()))
								fact.setTotalScaleUpVolume(order.getTotalScaleUpVolume());
							else
								fact.unsetTotalScaleUpVolume();
							if (!FieldUtil.isNull(order.getTotalScaleUpWeight()))
								fact.setTotalScaleUpWeight(order.getTotalScaleUpWeight());
							else
								fact.unsetTotalScaleUpWeight();
							if (!FieldUtil.isNull(order.getTotalInvoiceAmount()))
								fact.setTotalInvoiceAmount(order.getTotalInvoiceAmount());
							else
								fact.unsetTotalInvoiceAmount();
							fact.setInvoiceCurrency(ds.getInvoiceCurrency());
							fact.setCurrency(line.getCurrency());
							fact.setPromiseStatus(order.getPromiseStatus());
							fact.setRSPromiseStatus(rs.getRsPromiseStatus());
							fact.setDSPromiseStatus(ds.getDsPromiseStatus());
							if (!FieldUtil.isNull(order.getSysRequisitionId()))
								fact.setRequisitionId(order.getSysRequisitionId());
							else
								fact.unsetRequisitionId();
							if (!FieldUtil.isNull(line.getSysLnProgramId())) {
								fact.setLnProgramId(line.getSysLnProgramId());
							} else {
								fact.unsetLnProgramId();
							}

							fact.setIsConsignment(order.isIsConsignment());
							fact.setIsSpot(order.isIsSpot());
							fact.setIsEmergency(order.isIsEmergency());
							fact.setIsVMI(order.isIsVMI());
							fact.setOrigin(order.getOrigin());
							if (!FieldUtil.isNull(orderMdfs.getSysContractId()))
								fact.setContractId(orderMdfs.getSysContractId());
							else
								fact.unsetContractId();
							fact.setContractNumber(orderMdfs.getContractNumber());
							if (!FieldUtil.isNull(orderMdfs.getSysContractTermsId()))
								fact.setContractTermId(orderMdfs.getSysContractTermsId());
							else
								fact.unsetContractTermId();
							fact.setContractTermNumber(orderMdfs.getContractTermsNumber());
							if (!FieldUtil.isNull(lineMdfs.getSysContractLineId()))
								fact.setContractLineId(lineMdfs.getSysContractLineId());
							else
								fact.unsetContractLineId();
							fact.setContractLineNumber(lineMdfs.getContractLineLineNumber());
							fact.setIsExpedite(order.isIsExpedite());
							if (!FieldUtil.isNull(order.getSysParentOrderId()))
								fact.setParentOrderId(order.getSysParentOrderId());
							else
								fact.unsetParentOrderId();
							fact.setParentOrderNumber(order.getParentOrderOrderNumber());
							fact.setExtParentOrderNumber(order.getExtParentOrderNumber());
							fact.setOMSLnCancelCollabStatus(lineMdfs.getLnCancelCollaborationStatus());
							fact.setCancelCollabStatus(orderMdfs.getCancelCollaborationStatus());
							fact.setRsCancelCollabStatus(rsMdfs.getRsCancelCollaborationStatus());
							if (!FieldUtil.isNull(ds.getSysPromiseItemId()))
								fact.setPromiseItemId(ds.getSysPromiseItemId());
							else
								fact.unsetPromiseItemId();
							ItemRow promiseItemRow = null;
							if (!FieldUtil.isNull(ds.getSysPromiseItemId())) {
								promiseItemRow = ItemCacheManager.getInstance().getItem(ds.getSysPromiseItemId());
								fact.setPromiseItemDescription(promiseItemRow.getDescription());
								fact.setPromiseItemName(promiseItemRow.getItemName());
								fact.setPromiseItemEntName(promiseItemRow.getEntName());
							} else {
								fact.setPromiseItemDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setPromiseItemName(DvceConstants.NULL_STRING_VALUE);
								fact.setPromiseItemEntName(DvceConstants.NULL_STRING_VALUE);
							}
							fact.setShipWithGrpRef(ds.getShipWithGroupRef());
							fact.setExtShipToSiteName(rs.getExtShipToSiteName());
							fact.setExtShipToLocationName(rs.getExtShipToLocationName());
							fact.setExtShipFromSiteName(ds.getExtShipFromSiteName());
							fact.setExtShipFromLocationName(ds.getExtShipFromLocationName());
							if (!FieldUtil.isNull(rs.getSysShipToSiteId()))
								fact.setPartnerSiteName(
										OMSUtil.getPartnerSiteName(rs.getSysShipToSiteId(), vcDvceContext));
							else
								fact.setPartnerSiteName(DvceConstants.NULL_STRING_VALUE);
							fact.setOrderClassification(order.getOrderClassification());
							fact.setLevelModifiedDate(ds.getLevelModifiedDate());
							fact.setBPONumber(order.getBPONumber());
							fact.setBPOLineNumber(line.getBPOLineNumber());
							if (!FieldUtil.isNull(order.getSysProgramId()))
								fact.setProgramId(order.getSysProgramId());
							else
								fact.unsetProgramId();
							fact.setProgramName(order.getProgramName());
							fact.setProgramEntName(order.getProgramEnterpriseName());
							if (lineMdfs.isSetPricePer())
								fact.setPricePer(lineMdfs.getPricePer());
							else
								fact.unsetPricePer();
							if (line.isSetLineAmount())
								fact.setLineAmount(line.getLineAmount());
							else
								fact.unsetLineAmount();
							fact.setExternalDocNumber(ds.getExternalDocNumber());
							fact.setOverrideReasonCode(ds.getOverrideReasonCode());
							fact.setExtFreightForwarderName(order.getExtFreightForwarderName());
							fact.setBuyerCode(order.getBuyerCode());
							fact.setPlannerCode(order.getPlannerCode());
							fact.setInternational(order.isInternational());
							fact.setIsDutyFree(line.isIsDutyFree());
							fact.setRSIsExpedite(rs.isRsIsExpedite());
							if (dsMdfs.isSetNettedOrderedQty())
								fact.setNettedOrderedQty(dsMdfs.getNettedOrderedQty());
							else
								fact.unsetNettedOrderedQty();
							if (dsMdfs.isSetNettedReceivedQty())
								fact.setNettedReceivedQty(dsMdfs.getNettedReceivedQty());
							else
								fact.unsetNettedReceivedQty();
							fact.setFOBCode(orderMdfs.getFOBCode());
							if (pglrRow != null) {
								fact.setProductGroupLevelId(line.getSysProductGroupLevelId());
								fact.setProductGroupLevel1Name(pglrRow.getLevel1Name());
								fact.setProductGroupLevel2Name(pglrRow.getLevel2Name());
								fact.setProductGroupLevel3Name(pglrRow.getLevel3Name());
								fact.setProductGroupLevel4Name(pglrRow.getLevel4Name());
								fact.setProductGroupLevel5Name(pglrRow.getLevel5Name());
								fact.setProductGroupLevelEntName(pglrRow.getEntName());
								fact.setProductGroupTypeName(pglrRow.getTypeName());
								fact.setLevelNo(pglrRow.getLevelNo());
								fact.setLevelName(pglrRow.getLevelName());
								fact.setLevelDesc(pglrRow.getLevelDesc());
							} else {
								fact.unsetProductGroupLevelId();
								fact.setProductGroupLevel1Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevel2Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevel3Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevel4Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevel5Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevelEntName(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupTypeName(DvceConstants.NULL_STRING_VALUE);
								fact.unsetLevelNo();
								fact.setLevelName(DvceConstants.NULL_STRING_VALUE);
								fact.setLevelDesc(DvceConstants.NULL_STRING_VALUE);
							}

							fact.setExtEndCustomerPONo(dsMdfs.getExtEndCustomerPONo());
							fact.setFOBPoint(orderMdfs.getFOBPoint());
							fact.setIsHazardous(dsMdfs.isIsHazardous());
							fact.setPaymentTermsCode(orderMdfs.getPaymentTermsCode());
							if (!FieldUtil.isNull(orderMdfs.getSysPaymentTermsId()))
								fact.setPaymentTermsId(orderMdfs.getSysPaymentTermsId());
							else
								fact.unsetPaymentTermsId();
							fact.setPaymentTermsEnterpriseName(orderMdfs.getPaymentTermsEnterpriseName());
							fact.setDeviationReasonCode(dsMdfs.getDeviationReasonCode());
							if (dsMdfs.isSetPromisePricePer())
								fact.setPromisePricePer(dsMdfs.getPromisePricePer());
							else
								fact.unsetPromisePricePer();
							if (dsMdfs.isSetRequestPricePer())
								fact.setRequestPricePer(dsMdfs.getRequestPricePer());
							else
								fact.unsetRequestPricePer();
							if (dsMdfs.isSetAgreedPricePer())
								fact.setAgreedPricePer(dsMdfs.getAgreedPricePer());
							else
								fact.unsetAgreedPricePer();
							if (dsMdfs.isSetTotalCost())
								fact.setTotalCost(dsMdfs.getTotalCost());
							else
								fact.unsetTotalCost();
							fact.setBackOrderReasonCode(dsMdfs.getBackOrderReasonCode());
							fact.setAutoMoveToReceived(orderMdfs.isAutoMoveToReceived());
							fact.setIncoTermsLocation(orderMdfs.getIncoTermsLocation());
							fact.setASNCreationDate(ds.getASNCreationDate());
							fact.setReceivingWhseStatus(ds.getReceivingWhseStatus());
							fact.setShippingWhseStatus(ds.getShippingWhseStatus());
							fact.setShipmentStatus(ds.getShipmentStatus());
							if (!FieldUtil.isNull(rs.getSysShipToLocationId()))
								fact.setShipToLocationId(rs.getSysShipToLocationId());
							else
								fact.unsetShipToLocationId();
							fact.setShipToLocationName(rs.getShipToLocationName());
							fact.setShipToLocationAddress(rs.getShipToLocationAddress());
							fact.setShipToLocationSiteName(rs.getShipToLocationSiteName());
							if (!FieldUtil.isNull(ds.getSysShipFromLocationId()))
								fact.setShipFromLocationId(ds.getSysShipFromLocationId());
							else
								fact.unsetShipFromLocationId();
							fact.setShipFromLocationName(ds.getShipFromLocationName());
							fact.setShipFromLocationAddress(ds.getShipFromLocationAddress());
							fact.setShipFromLocationSiteName(ds.getShipFromLocationSiteName());
							fact.setExtShipToLocationName(rs.getExtShipToLocationName());
							fact.setExtShipFromLocationName(ds.getExtShipFromLocationName());
							SiteRow shipFromSite = null;
							if (!FieldUtil.isNull(ds.getSysShipFromSiteId())) {
								shipFromSite = SiteCacheManager.getInstance().getSite(ds.getSysShipFromSiteId(),
										vcDvceContext);
							}
							if (shipToSite != null) {
								fact.setShipToSiteId(shipToSite.getSysSiteId());
								fact.setShipToSiteName(shipToSite.getSiteName());
								fact.setShipToDescription(shipToSite.getDescription());
								fact.setShipToTimeZoneId(shipToSite.getTimeZoneId());
							} else {
								fact.unsetShipToSiteId();
								fact.setShipToSiteName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToTimeZoneId(DvceConstants.NULL_STRING_VALUE);
							}
							if (shipFromSite != null) {
								fact.setShipFromSiteId(shipFromSite.getSysSiteId());
								fact.setShipFromSiteName(shipFromSite.getSiteName());
								fact.setShipFromDescription(shipFromSite.getDescription());
								fact.setShipFromTimeZoneId(shipFromSite.getTimeZoneId());
							} else {
								fact.unsetShipFromSiteId();
								fact.setShipFromSiteName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromTimeZoneId(DvceConstants.NULL_STRING_VALUE);
							}
							fact.setExtBPONumber(fact.getExtBPONumber());
							fact.setLnCurrency(line.getCurrency());
							if (order.isSetTotalQuantity())
								fact.setTotalQuantity(order.getTotalQuantity());
							else
								fact.unsetTotalQuantity();
							if (rs.isSetDemandQuantity())
								fact.setDemandQty(rs.getDemandQuantity());
							else
								fact.unsetDemandQty();
							if (rs.isSetOrigRequestQuantity())
								fact.setOrigRequestQty(rs.getOrigRequestQuantity());
							else
								fact.unsetOrigRequestQty();
							fact.setOverrideComment(ds.getOverrideComment());
							if (rs.isSetDemandScaleUpQuantity())
								fact.setDemandScaleUpQuantity(rs.getDemandScaleUpQuantity());
							else
								fact.unsetDemandScaleUpQuantity();
							if (ds.isSetReturnedQuantity())
								fact.setReturnedQuantity(ds.getReturnedQuantity());
							else
								fact.unsetReturnedQuantity();
							if (ds.isSetRejectedQuantity())
								fact.setRejectedQuantity(ds.getRejectedQuantity());
							else
								fact.unsetRejectedQuantity();
							if (rs.isSetProjectedDaysOfSupply())
								fact.setProjectedDaysOfSupply(rs.getProjectedDaysOfSupply());
							else
								fact.unsetProjectedDaysOfSupply();
							if (order.isSetTotalAmount())
								fact.setTotalAmount(order.getTotalAmount());
							else
								fact.unsetTotalAmount();
							fact.setExtVendorItemName(line.getExtVendorItemName());
				              SiteRow consolidationSite = null;
				              if(!FieldUtil.isNull(ds.getSysConsolidationSiteId())) {
				                consolidationSite = SiteCacheManager.getInstance().getSite(ds.getSysConsolidationSiteId(), context);
				                if(consolidationSite != null) {
				                  fact.setConsolidationSiteId(consolidationSite.getSysSiteId());
				                  fact.setConsolidationSiteName(consolidationSite.getSiteName());
				                  fact.setConsolidationSiteDescription(consolidationSite.getDescription());
				                  fact.setConsolidationSiteTimeZoneId(consolidationSite.getTimeZoneId());
				                } else {
				                  fact.unsetConsolidationSiteId();
				                  fact.setConsolidationSiteName(DvceConstants.NULL_STRING_VALUE);
				                  fact.setConsolidationSiteDescription(DvceConstants.NULL_STRING_VALUE);
				                  fact.setConsolidationSiteTimeZoneId(DvceConstants.NULL_STRING_VALUE);
				                }
				              }
				              fact.setConsolidationDeliveryDate(ds.getConsolidationDeliveryDate());
				              fact.setConsolidationPickupDate(ds.getConsolidationPickupDate());
				              soOrderFact.add(fact);
						} else if (OrderTypeEnum.DEPLOYMENT_ORDER.stringValue().equals(order.getOrderType())) {
							DeploymentOrderFact fact = new DeploymentOrderFact();
							fact.setOrderId(order.getSysId());
							fact.setLineId(line.getSysId());
							fact.setRequestScheduleId(rs.getSysId());
							fact.setDeliveryScheduleId(ds.getSysId());
							fact.setOrderNumber(order.getOrderNumber());
							fact.setLineNumber(line.getLineNumber());
							fact.setRequestScheduleNumber(rs.getRequestScheduleNumber());
							fact.setDeliveryScheduleNumber(ds.getDeliveryScheduleNumber());
							if (!FieldUtil.isNull(line.getSpecificItemName()))
								fact.setSpecificItemName(line.getSpecificItemName());
							else
								fact.setSpecificItemName(DvceConstants.NULL_STRING_VALUE);
							if (!FieldUtil.isNull(line.getGenericItemName()))
								fact.setGenericItemName(line.getGenericItemName());
							else
								fact.setGenericItemName(DvceConstants.NULL_STRING_VALUE);
							if (!FieldUtil.isNull(ds.getSysShipFromSiteId())) {
								fact.setShipFromSiteId(ds.getSysShipFromSiteId());
								SiteRow shipFromSiteRow = SiteCacheManager.getInstance()
										.getSite(ds.getSysShipFromSiteId(), vcDvceContext);
								fact.setShipFromSiteName(shipFromSiteRow.getSiteName());
								fact.setShipFromDescription(shipFromSiteRow.getDescription());
								fact.setShipFromIsDC(shipFromSiteRow.isIsDc());
								fact.setShipFromIsPlant(shipFromSiteRow.isIsPlant());
							} else {
								fact.unsetShipFromSiteId();
								fact.setShipFromSiteName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromDescription(DvceConstants.NULL_STRING_VALUE);
								fact.unsetShipFromIsDC();
								fact.unsetShipFromIsPlant();
							}
							if (!FieldUtil.isNull(rs.getSysShipToSiteId())) {
								SiteRow shipToSiteRow = SiteCacheManager.getInstance().getSite(rs.getSysShipToSiteId(),
										vcDvceContext);
								fact.setShipToSiteId(rs.getSysShipToSiteId());
								fact.setShipToDescription(shipToSiteRow.getDescription());
								fact.setShipToSiteName(shipToSiteRow.getSiteName());
								fact.setShiptoSiteIsPlant(shipToSiteRow.isIsDc());
								fact.setShiptoSiteIsDC(shipToSiteRow.isIsPlant());
							} else {
								fact.unsetShipToSiteId();
								fact.setShipToDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToSiteName(DvceConstants.NULL_STRING_VALUE);
								fact.unsetShiptoSiteIsPlant();
								fact.unsetShiptoSiteIsDC();
							}
							if (buyingOrg != null) {
								fact.setBuyingOrgId(order.getSysBuyingOrgId());
								fact.setBuyingOrgDescription(buyingOrg.getDescription());
								fact.setBuyingOrgName(buyingOrg.getOrgName());
								fact.setBuyingOrgEntName(buyingOrg.getEntName());
							} else {
								fact.unsetBuyingOrgId();
								fact.setBuyingOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setBuyingOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setBuyingOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (sellingOrg != null) {
								fact.setSellingOrgId(order.getSysSellingOrgId());
								fact.setSellingOrgDescription(sellingOrg.getDescription());
								fact.setSellingOrgName(sellingOrg.getOrgName());
								fact.setSellingOrgEntName(sellingOrg.getEntName());
							} else {
								fact.unsetSellingOrgId();
								fact.setSellingOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setSellingOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setSellingOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (owningOrg != null) {
								fact.setOwningOrgId(order.getSysOwningOrgId());
								fact.setOwningOrgDescription(owningOrg.getDescription());
								fact.setOwningOrgName(owningOrg.getOrgName());
								fact.setOwningOrgEntName(owningOrg.getEntName());
							} else {
								fact.unsetOwningOrgId();
								fact.setOwningOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setOwningOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setOwningOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (custOfBuyerOrg != null) {
								fact.setCustomerOfBuyerOrgId(order.getSysCustOfBuyerOrgId());
								fact.setCustomerOfBuyerDescription(custOfBuyerOrg.getDescription());
								fact.setCustOfBuyerOrgName(custOfBuyerOrg.getOrgName());
								fact.setCustOfBuyerOrgEntName(custOfBuyerOrg.getEntName());
							} else {
								fact.unsetCustomerOfBuyerOrgId();
								fact.setCustomerOfBuyerDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setCustOfBuyerOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setCustOfBuyerOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (omoOrg != null) {
								fact.setOMOOrgId(order.getSysOrderMgmtOrgId());
								fact.setOMOOrgDescription(omoOrg.getDescription());
								fact.setOMOOrgName(omoOrg.getOrgName());
								fact.setOMOOrgEntName(omoOrg.getEntName());
							} else {
								fact.unsetOMOOrgId();
								fact.setOMOOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setOMOOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setOMOOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (fulFillmentOrg != null) {
								fact.setFulfillmentOrgId(order.getSysFulfillmentOrgId());
								fact.setFulfillmentOrgDescription(fulFillmentOrg.getDescription());
								fact.setFulfillmentOrgName(fulFillmentOrg.getOrgName());
								fact.setFulfillmentOrgEntName(fulFillmentOrg.getEntName());
							} else {
								fact.unsetFulfillmentOrgId();
								fact.setFulfillmentOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setFulfillmentOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setFulfillmentOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (freightFwdOrg != null) {
								fact.setFreightFwdOrgId(order.getSysFreightFwdOrgId());
								fact.setFreightFwdOrgDescription(freightFwdOrg.getDescription());
								fact.setFreightFwdOrgName(freightFwdOrg.getOrgName());
								fact.setFreightFwdOrgEntName(freightFwdOrg.getEntName());
							} else {
								fact.unsetFreightFwdOrgId();
								fact.setFreightFwdOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setFreightFwdOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setFreightFwdOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (shipFromOrg != null) {
								fact.setShipFromOrgId(order.getSysShipFromOrgId());
								fact.setShipFromOrgDescription(shipFromOrg.getDescription());
								fact.setShipFromOrgName(shipFromOrg.getOrgName());
								fact.setShipFromOrgEntName(shipFromOrg.getEntName());
							} else {
								fact.unsetShipFromOrgId();
								fact.setShipFromOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (shipToOrg != null) {
								fact.setShipToOrgId(order.getSysShipToOrgId());
								fact.setShipToOrgDescription(shipToOrg.getDescription());
								fact.setShipToOrgName(shipToOrg.getOrgName());
								fact.setShipToOrgEntName(shipToOrg.getEntName());
							} else {
								fact.unsetShipToOrgId();
								fact.setShipToOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
              if (!FieldUtil.isNull(rs.getSysRsShipToOrgId())) {
							fact.setRsShipToOrgId(rs.getSysRsShipToOrgId());
              } else {
                fact.setRsShipToOrgId(DvceConstants.NULL_LONG_VALUE);
              }
              if (!FieldUtil.isNull(rs.getRsShipToOrgName())) {
							fact.setRsShipToOrgName(rs.getRsShipToOrgName());
              } else {
                fact.setRsShipToOrgName(DvceConstants.NULL_STRING_VALUE);
              }
              if (!FieldUtil.isNull(rs.getShipToAddress()))
                 fact.setShipToAddress(rs.getShipToAddress());
              else {
                fact.setShipToAddress(DvceConstants.NULL_ADDRESS_VALUE);
              }                 
              if (!FieldUtil.isNull(ds.getShipFromAddress()))
                 fact.setShipFromAddress(ds.getShipFromAddress());
              else {
                 fact.setShipFromAddress(DvceConstants.NULL_ADDRESS_VALUE);
              }  
							if (tcoOrg != null) {
								fact.setTCOOrgId(order.getSysTCOOrgId());
								fact.setTCOrgDescription(tcoOrg.getDescription());
								fact.setTCOrgName(tcoOrg.getOrgName());
								fact.setTCOrgEntName(tcoOrg.getEntName());
							} else {
								fact.unsetTCOOrgId();
								fact.setTCOrgDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setTCOrgName(DvceConstants.NULL_STRING_VALUE);
								fact.setTCOrgEntName(DvceConstants.NULL_STRING_VALUE);
							}
							if (itemRow != null) {
								fact.setItemId(line.getSysItemId());
								fact.setItemDescription(itemRow.getDescription());
								fact.setItemName(itemRow.getItemName());
								fact.setItemEnterpriseName(itemRow.getEntName());
								fact.setExtMfgItemName(itemRow.getExtMfgItemName());
							} else {
								fact.unsetItemId();
								fact.setItemDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setItemName(DvceConstants.NULL_STRING_VALUE);
								fact.setItemEnterpriseName(DvceConstants.NULL_STRING_VALUE);
								fact.setExtMfgItemName(DvceConstants.NULL_STRING_VALUE);
							}
							if (partnerRow != null) {
								fact.setVendorId(order.getSysVendorId());
								fact.setVendorDescription(partnerRow.getDescription());
								fact.setVendorName(partnerRow.getPartnerName());
								fact.setVendorOrgName(partnerRow.getPartnerOrgName());
							} else {
								fact.unsetVendorId();
								fact.setVendorDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setVendorName(DvceConstants.NULL_STRING_VALUE);
								fact.setVendorName(DvceConstants.NULL_STRING_VALUE);
							}
							fact.setRemitToAddress(order.getRemitToAddress());
							fact.setBillToAddress(order.getBillToAddress());
							fact.setLineType(line.getLineType());
							if (!FieldUtil.isNull(line.getUnitPrice()))
								fact.setUnitPrice(line.getUnitPrice());
							else
								fact.unsetUnitPrice();
							fact.setSellingAgent1(order.getSellingAgent1Name());
							fact.setSellingAgent2(order.getSellingAgent2Name());
							fact.setSellingAgent3(order.getSellingAgent3Name());
							fact.setSellingAgent4(order.getSellingAgent4Name());
							fact.setBuyingAgent1(order.getBuyingAgent1Name());
							fact.setBuyingAgent2(order.getBuyingAgent2Name());
							fact.setBuyingAgent3(order.getBuyingAgent3Name());
							fact.setBuyingAgent4(order.getBuyingAgent4Name());
							fact.setSellingAgent1EntName(order.getSellingAgent1EnterpriseName());
							fact.setSellingAgent2EntName(order.getSellingAgent2EnterpriseName());
							fact.setSellingAgent3EntName(order.getSellingAgent3EnterpriseName());
							fact.setSellingAgent4EntName(order.getSellingAgent4EnterpriseName());
							fact.setBuyingAgent1EntName(order.getBuyingAgent1EnterpriseName());
							fact.setBuyingAgent2EntName(order.getBuyingAgent2EnterpriseName());
							fact.setBuyingAgent3EntName(order.getBuyingAgent3EnterpriseName());
							fact.setBuyingAgent4EntName(order.getBuyingAgent4EnterpriseName());
							fact.setIsPendingAuthorization(orderMdfs.isIsPendingAuthorization());
							if (!FieldUtil.isNull(line.getSysVendorItemId()))
								fact.setVendorItemId(line.getSysVendorItemId());
							else
								fact.unsetVendorItemId();
							if (!FieldUtil.isNull(line.getSysGenericItemId()))
								fact.setGenericItemId(line.getSysGenericItemId());
							else
								fact.unsetGenericItemId();
							if (!FieldUtil.isNull(line.getSysSpecificItemId()))
								fact.setSpecificItemId(line.getSysSpecificItemId());
							else
								fact.unsetSpecificItemId();
							if (!FieldUtil.isNull(order.getSysSellingAgent1Id()))
								fact.setSellingAgent1Id(order.getSysSellingAgent1Id());
							else
								fact.unsetSellingAgent1Id();
							if (!FieldUtil.isNull(order.getSysSellingAgent2Id()))
								fact.setSellingAgent2Id(order.getSysSellingAgent2Id());
							else
								fact.unsetSellingAgent2Id();
							if (!FieldUtil.isNull(order.getSysSellingAgent3Id()))
								fact.setSellingAgent3Id(order.getSysSellingAgent3Id());
							else
								fact.unsetSellingAgent3Id();
							if (!FieldUtil.isNull(order.getSysSellingAgent4Id()))
								fact.setSellingAgent4Id(order.getSysSellingAgent4Id());
							else
								fact.unsetSellingAgent4Id();
							if (!FieldUtil.isNull(order.getSysBuyingAgent1Id()))
								fact.setBuyingAgent1Id(order.getSysBuyingAgent1Id());
							else
								fact.unsetBuyingAgent1Id();
							if (!FieldUtil.isNull(order.getSysBuyingAgent2Id()))
								fact.setBuyingAgent2Id(order.getSysBuyingAgent2Id());
							else
								fact.unsetBuyingAgent2Id();
							if (!FieldUtil.isNull(order.getSysBuyingAgent3Id()))
								fact.setBuyingAgent3Id(order.getSysBuyingAgent3Id());
							else
								fact.unsetBuyingAgent3Id();
							if (!FieldUtil.isNull(order.getSysBuyingAgent4Id()))
								fact.setBuyingAgent4Id(order.getSysBuyingAgent4Id());
							else
								fact.unsetBuyingAgent4Id();
							if (!FieldUtil.isNull(ds.getSysDsShipFromOrgId()))
								fact.setDsShipFromOrgId(ds.getSysDsShipFromOrgId());
							else
								fact.unsetDsShipFromOrgId();
							DeliveryScheduleMDFs dsMdfs = ds.getMDFs(DeliveryScheduleMDFs.class);
							if (!FieldUtil.isNull(order.getSysClonedFromOrderId()))
								fact.setClonedFromOrderId(order.getSysClonedFromOrderId());
							else
								fact.unsetClonedFromOrderId();
							fact.setOrderSubType(order.getOrderSubType());
							fact.setVendorRejectReasonCode(ds.getVendorRejectReasonCode());
							fact.setOrigRequestQtyUOM(rs.getOriginalRequestQuantityUOM());
							fact.setSalesOrderNumber(order.getSalesOrderNumber());
							fact.setPurchaseOrderNumber(order.getPurchaseOrderNumber());
							fact.setOMOOrderNumber(order.getOmoOrderNumber());
							fact.setWMSOrderNumber(order.getWmsOrderNumber());
							fact.setSubmitForApprovalDate(order.getSubmitForApprovalDate());
							fact.setDeliveryGroupNumber(ds.getDeliveryGroupNumber());
							if (!FieldUtil.isNull(order.getSysTransModeId()))
								fact.setTransModeId(order.getSysTransModeId());
							else
								fact.unsetTransModeId();
							if (!FieldUtil.isNull(rs.getSysRsTransModeId()))
								fact.setRsTransModeId(rs.getSysRsTransModeId());
							else
								fact.unsetRsTransModeId();
							// Date fields
							if (!FieldUtil.isNull(order.getSysCreationOrgId()))
								fact.setCreationOrgId(order.getSysCreationOrgId());
							fact.setRequestDeliveryDate(ds.getRequestDeliveryDate());
							fact.setPromiseDeliveryDate(ds.getPromiseDeliveryDate());
							fact.setASNCreationDate(ds.getASNCreationDate());
							fact.setCreationDate(order.getCreationDate());
							fact.setTransMode(order.getTransModeName());
							fact.setIncoTerms(order.getIncoTerms());
							fact.setExtItemName(line.getExtItemName());
							fact.setBuyerOrderApprovalDate(order.getBuyerOrderApprovalDate());
							fact.setOrigPromiseDeliveryDate(ds.getOrigPromiseDeliveryDate());
							fact.setOrigPromiseShipDate(ds.getOrigPromiseShipDate());
							fact.setOrigRequestShipDate(rs.getOrigRequestShipDate());
							fact.setOrigRequestDeliveryDate(rs.getOrigRequestDeliveryDate());
							fact.setOverriddenDeliveryDate(ds.getOverriddenDeliveryDate());
							fact.setOverriddenShipDate(ds.getOverriddenShipDate());
							fact.setPlannedShipDate(ds.getPlannedShipDate());
							fact.setWhseReleaseTargetDate(ds.getWhseReleaseTargetDate());
							fact.setWarehouseReleaseDate(ds.getWarehouseReleaseDate());
							fact.setVendorOrderApprovalDate(order.getVendorOrderApprovalDate());
							fact.setVendorAckDate(order.getVendorAckDate());
							fact.setTMSReleaseTargetDate(ds.getTMSReleaseTargetDate());
							fact.setTMSReleaseDate(ds.getTMSReleaseDate());
							fact.setRequestMinItemExpiryDate(ds.getRequestMinItemExpiryDate());
							fact.setRequestIncoDateStartDate(ds.getRequestIncoDateStartDate());
							fact.setRequestIncoDateEndDate(ds.getRequestIncoDateEndDate());
							fact.setPromiseMinItemExpiryDate(ds.getPromiseMinItemExpiryDate());
							fact.setPromiseExpiryDate(dsMdfs.getPromiseExpiryDate());
							fact.setAgreedDeliveryDate(ds.getAgreedDeliveryDate());
							fact.setAgreedIncoDateEndDate(ds.getAgreedIncoDateEndDate());
							fact.setActualReceiptDate(ds.getActualReceiptDate());
							fact.setAgreedIncoDateStartDate(ds.getAgreedIncoDateStartDate());
							fact.setAgreedMinItemExpiryDate(ds.getAgreedMinItemExpiryDate());
							fact.setActualDeliveryDate(ds.getActualDeliveryDate());
							fact.setAgreedShipDate(ds.getAgreedShipDate());
							fact.setPromiseShipDate(ds.getPromiseShipDate());
							fact.setRequestShipDate(ds.getRequestShipDate());
							if (!FieldUtil.isNull(ds.getAgreedDeliveryDate())) {
								fact.setAgreedDeliveryDateLocal(getLocalFormatForDate(ds.getAgreedDeliveryDate()));
							} else {
								fact.setAgreedDeliveryDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getPromiseDeliveryDate())) {
								fact.setPromiseDeliveryDateLocal(getLocalFormatForDate(ds.getPromiseDeliveryDate()));
							} else {
								fact.setPromiseDeliveryDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getRequestDeliveryDate())) {
								fact.setRequestDeliveryDateLocal(getLocalFormatForDate(ds.getRequestDeliveryDate()));
							} else {
								fact.setRequestDeliveryDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getAgreedDeliveryDate())) {
								fact.setDeliveryDateLocal(getLocalFormatForDate(ds.getAgreedDeliveryDate()));
							} else if (!FieldUtil.isNull(ds.getPromiseDeliveryDate())) {
								fact.setDeliveryDateLocal(getLocalFormatForDate(ds.getPromiseDeliveryDate()));
							} else if (!FieldUtil.isNull(ds.getRequestDeliveryDate())) {
								fact.setDeliveryDateLocal(getLocalFormatForDate(ds.getRequestDeliveryDate()));
							} else {
								fact.setDeliveryDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getAgreedShipDate())) {
								fact.setAgreedShipDateLocal(getLocalFormatForDate(ds.getAgreedShipDate()));
							} else {
								fact.setAgreedShipDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getPromiseShipDate())) {
								fact.setPromiseShipDateLocal(getLocalFormatForDate(ds.getPromiseShipDate()));
							} else {
								fact.setPromiseShipDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getRequestShipDate())) {
								fact.setRequestShipDateLocal(getLocalFormatForDate(ds.getRequestShipDate()));
							} else {
								fact.setRequestShipDateLocal(0);
							}
							if (!FieldUtil.isNull(ds.getAgreedShipDate())) {
								fact.setShipDateLocal(getLocalFormatForDate(ds.getAgreedShipDate()));
							} else if (!FieldUtil.isNull(ds.getPromiseShipDate())) {
								fact.setShipDateLocal(getLocalFormatForDate(ds.getPromiseShipDate()));
							} else if (!FieldUtil.isNull(ds.getRequestShipDate())) {
								fact.setShipDateLocal(getLocalFormatForDate(ds.getRequestShipDate()));
							} else {
								fact.setShipDateLocal(0);
							}

							if (!FieldUtil.isNull(order.getCreationDate())) {
								fact.setCreationDateLocal(getLocalFormatForDate(order.getCreationDate()));
							} else {
								fact.setCreationDateLocal(0);
							}
							// State feilds
							fact.setDSState(ds.getState());
							fact.setOrderState(order.getState());
							fact.setLineState(line.getState());
							fact.setRSState(rs.getState());
							// Quantity fields
							if (ds.isSetOverriddenAgreedQuantity())
								fact.setOverriddenAgreedQuantity(ds.getOverriddenAgreedQuantity());
							else
								fact.unsetOverriddenAgreedQuantity();
							if (ds.isSetAgreedQuantity())
								fact.setAgreedQty(ds.getAgreedQuantity());
							else
								fact.unsetAgreedQty();
							if (ds.isSetPromiseQuantity())
								fact.setPromiseQty(ds.getPromiseQuantity());
							else
								fact.unsetPromiseQty();
							if (ds.isSetRequestQuantity())
								fact.setRequestQty(ds.getRequestQuantity());
							else
								fact.unsetRequestQty();
							if (ds.isSetPlannedShipQuantity())
								fact.setPlannedShippedQty(ds.getPlannedShipQuantity());
							else
								fact.unsetPlannedShippedQty();
							if (ds.isSetShippedQuantity())
								fact.setShippedQty(ds.getShippedQuantity());
							else
								fact.unsetShippedQty();
							if (ds.isSetReceivedQuantity())
								fact.setReceivedQty(ds.getReceivedQuantity());
							else
								fact.unsetReceivedQty();
							if (ds.isSetInvoicedQuantity())
								fact.setInvoicedQty(ds.getInvoicedQuantity());
							else
								fact.unsetInvoicedQty();
							if (ds.isSetConsignmentQuantity())
								fact.setConsignmentQty(ds.getConsignmentQuantity());
							else
								fact.unsetConsignmentQty();
							if (ds.isSetConsignmentConsumedQuantity())
								fact.setConsigmentConsumedQty(ds.getConsignmentConsumedQuantity());
							else
								fact.unsetConsigmentConsumedQty();
							if (ds.isSetBackOrderQuantity())
								fact.setBackorderedQty(ds.getBackOrderQuantity());
							else
								fact.unsetBackorderedQty();
							if (ds.isSetCancelledQuantity())
								fact.setCancelledQty(ds.getCancelledQuantity());
							else
								fact.unsetCancelledQty();
							if (ds.isSetOrigPromisedQuantity())
								fact.setOrigPromisedQuantity(ds.getOrigPromisedQuantity());
							else
								fact.unsetOrigPromisedQuantity();
							if (!FieldUtil.isNull(order.getQuantityUom()))
								fact.setQuantityUOM(order.getQuantityUom());
							else
								fact.setQuantityUOM(DvceConstants.NULL_STRING_VALUE);
							if (!FieldUtil.isNull(line.getQuantityUOM()))
								fact.setLnQuantityUOM(line.getQuantityUOM());
							else
								fact.setLnQuantityUOM(DvceConstants.NULL_STRING_VALUE);
							if (dsMdfs.isSetNettedOrderedQty())
								fact.setNettedOrderedQty(dsMdfs.getNettedOrderedQty());
							else
								fact.unsetNettedOrderedQty();
							if (dsMdfs.isSetNettedReceivedQty())
								fact.setNettedReceivedQty(dsMdfs.getNettedReceivedQty());
							else
								fact.unsetNettedReceivedQty();
							// Amount
							if (ds.isSetFreightCost())
								fact.setFreightCost(ds.getFreightCost());
							else
								fact.unsetFreightCost();
							if (ds.isSetInvoiceAmount())
								fact.setInvoiceAmount(ds.getInvoiceAmount());
							else
								fact.unsetInvoiceAmount();
							if (ds.isSetInsuranceAmount())
								fact.setInsuranceAmount(ds.getInsuranceAmount());
							else
								fact.unsetInsuranceAmount();
							if (ds.isSetRequestUnitPriceAmount())
								fact.setRequestUnitPriceAmount(ds.getRequestUnitPriceAmount());
							else
								fact.unsetRequestUnitPriceAmount();
							if (ds.isSetPromiseUnitPriceAmount())
								fact.setPromiseUnitPriceAmount(ds.getPromiseUnitPriceAmount());
							else
								fact.unsetPromiseUnitPriceAmount();
							if (ds.isSetAgreedUnitPriceAmount())
								fact.setAgreedUnitPriceAmount(ds.getAgreedUnitPriceAmount());
							else
								fact.unsetAgreedUnitPriceAmount();
							if (ds.isSetOtherCostAmount())
								fact.setOtherCostAmount(ds.getOtherCostAmount());
							else
								fact.unsetOtherCostAmount();
							if (ds.isSetCustomsDutyAmount())
								fact.setCustomDutyAmount(ds.getCustomsDutyAmount());
							else
								fact.unsetCustomDutyAmount();
							if (ds.isSetTaxAmount())
								fact.setTaxAmount(ds.getTaxAmount());
							else
								fact.unsetTaxAmount();
							if (dsMdfs.isSetConvAgreedQtyAmount())
								fact.setConvAgreedQtyAmount(dsMdfs.getConvAgreedQtyAmount());
							else
								fact.unsetConvAgreedQtyAmount();
							fact.setConvAgreedQtyUOM(dsMdfs.getConvAgreedQtyUOM());
							if (!FieldUtil.isNull(order.getTotalWeight()))
								fact.setTotalWeight(order.getTotalWeight());
							else
								fact.unsetTotalWeight();
							fact.setWeightUOM(order.getWeightUom());
							if (!FieldUtil.isNull(order.getTotalVolume()))
								fact.setTotalVolume(order.getTotalVolume());
							else
								fact.unsetTotalVolume();
							fact.setVolumeUOM(order.getVolumeUom());
							if (!FieldUtil.isNull(order.getTotalScaleUpQuantity()))
								fact.setTotalScaleUpQuantity(order.getTotalScaleUpQuantity());
							else
								fact.unsetTotalScaleUpQuantity();
							if (!FieldUtil.isNull(order.getTotalScaleUpVolume()))
								fact.setTotalScaleUpVolume(order.getTotalScaleUpVolume());
							else
								fact.unsetTotalScaleUpVolume();
							if (!FieldUtil.isNull(order.getTotalScaleUpWeight()))
								fact.setTotalScaleUpWeight(order.getTotalScaleUpWeight());
							else
								fact.unsetTotalScaleUpWeight();
							if (!FieldUtil.isNull(order.getTotalInvoiceAmount()))
								fact.setTotalInvoiceAmount(order.getTotalInvoiceAmount());
							else
								fact.unsetTotalInvoiceAmount();
							fact.setInvoiceCurrency(ds.getInvoiceCurrency());
							fact.setCurrency(line.getCurrency());
							fact.setPromiseStatus(order.getPromiseStatus());
							fact.setRSPromiseStatus(rs.getRsPromiseStatus());
							fact.setDSPromiseStatus(ds.getDsPromiseStatus());
							if (!FieldUtil.isNull(order.getSysRequisitionId()))
								fact.setRequisitionId(order.getSysRequisitionId());
							else
								fact.unsetRequisitionId();
							if (!FieldUtil.isNull(line.getSysLnProgramId())) {
								fact.setLnProgramId(line.getSysLnProgramId());
							} else {
								fact.unsetLnProgramId();
							}

							fact.setIsConsignment(order.isIsConsignment());
							fact.setIsSpot(order.isIsSpot());
							fact.setIsEmergency(order.isIsEmergency());
							fact.setIsVMI(order.isIsVMI());
							fact.setOrigin(order.getOrigin());
							if (!FieldUtil.isNull(orderMdfs.getSysContractId()))
								fact.setContractId(orderMdfs.getSysContractId());
							else
								fact.unsetContractId();
							fact.setContractNumber(orderMdfs.getContractNumber());
							if (!FieldUtil.isNull(orderMdfs.getSysContractTermsId()))
								fact.setContractTermId(orderMdfs.getSysContractTermsId());
							else
								fact.unsetContractTermId();
							fact.setContractTermNumber(orderMdfs.getContractTermsNumber());
							if (!FieldUtil.isNull(lineMdfs.getSysContractLineId()))
								fact.setContractLineId(lineMdfs.getSysContractLineId());
							else
								fact.unsetContractLineId();
							fact.setContractLineNumber(lineMdfs.getContractLineLineNumber());
							fact.setIsExpedite(order.isIsExpedite());
							if (!FieldUtil.isNull(order.getSysParentOrderId()))
								fact.setParentOrderId(order.getSysParentOrderId());
							else
								fact.unsetParentOrderId();
							fact.setParentOrderNumber(order.getParentOrderOrderNumber());
							fact.setExtParentOrderNumber(order.getExtParentOrderNumber());
							fact.setOMSLnCancelCollabStatus(lineMdfs.getLnCancelCollaborationStatus());
							fact.setCancelCollabStatus(orderMdfs.getCancelCollaborationStatus());
							fact.setRsCancelCollabStatus(rsMdfs.getRsCancelCollaborationStatus());
							if (!FieldUtil.isNull(ds.getSysPromiseItemId()))
								fact.setPromiseItemId(ds.getSysPromiseItemId());
							else
								fact.unsetPromiseItemId();
							ItemRow promiseItemRow = null;
							if (!FieldUtil.isNull(ds.getSysPromiseItemId())) {
								promiseItemRow = ItemCacheManager.getInstance().getItem(ds.getSysPromiseItemId());
								fact.setPromiseItemDescription(promiseItemRow.getDescription());
								fact.setPromiseItemName(promiseItemRow.getItemName());
								fact.setPromiseItemEntName(promiseItemRow.getEntName());
							} else {
								fact.setPromiseItemDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setPromiseItemName(DvceConstants.NULL_STRING_VALUE);
								fact.setPromiseItemEntName(DvceConstants.NULL_STRING_VALUE);
							}
							fact.setShipWithGrpRef(ds.getShipWithGroupRef());
							fact.setExtShipToSiteName(rs.getExtShipToSiteName());
							fact.setExtShipToLocationName(rs.getExtShipToLocationName());
							fact.setExtShipFromSiteName(ds.getExtShipFromSiteName());
							fact.setExtShipFromLocationName(ds.getExtShipFromLocationName());
							if (!FieldUtil.isNull(rs.getSysShipToSiteId()))
								fact.setPartnerSiteName(
										OMSUtil.getPartnerSiteName(rs.getSysShipToSiteId(), vcDvceContext));
							else
								fact.setPartnerSiteName(DvceConstants.NULL_STRING_VALUE);
							fact.setOrderClassification(order.getOrderClassification());
							fact.setLevelModifiedDate(ds.getLevelModifiedDate());
							fact.setBPONumber(order.getBPONumber());
							fact.setBPOLineNumber(line.getBPOLineNumber());
							if (!FieldUtil.isNull(order.getSysProgramId()))
								fact.setProgramId(order.getSysProgramId());
							else
								fact.unsetProgramId();
							fact.setProgramName(order.getProgramName());
							fact.setProgramEntName(order.getProgramEnterpriseName());
							if (lineMdfs.isSetPricePer())
								fact.setPricePer(lineMdfs.getPricePer());
							else
								fact.unsetPricePer();
							if (line.isSetLineAmount())
								fact.setLineAmount(line.getLineAmount());
							else
								fact.unsetLineAmount();
							fact.setExternalDocNumber(ds.getExternalDocNumber());
							fact.setOverrideReasonCode(ds.getOverrideReasonCode());
							fact.setExtFreightForwarderName(order.getExtFreightForwarderName());
							fact.setBuyerCode(order.getBuyerCode());
							fact.setPlannerCode(order.getPlannerCode());
							fact.setInternational(order.isInternational());
							fact.setIsDutyFree(line.isIsDutyFree());
							fact.setRSIsExpedite(rs.isRsIsExpedite());
							if (dsMdfs.isSetNettedOrderedQty())
								fact.setNettedOrderedQty(dsMdfs.getNettedOrderedQty());
							else
								fact.unsetNettedOrderedQty();
							if (dsMdfs.isSetNettedReceivedQty())
								fact.setNettedReceivedQty(dsMdfs.getNettedReceivedQty());
							else
								fact.unsetNettedReceivedQty();
							fact.setFOBCode(orderMdfs.getFOBCode());
							if (pglrRow != null) {
								fact.setProductGroupLevelId(line.getSysProductGroupLevelId());
								fact.setProductGroupLevel1Name(pglrRow.getLevel1Name());
								fact.setProductGroupLevel2Name(pglrRow.getLevel2Name());
								fact.setProductGroupLevel3Name(pglrRow.getLevel3Name());
								fact.setProductGroupLevel4Name(pglrRow.getLevel4Name());
								fact.setProductGroupLevel5Name(pglrRow.getLevel5Name());
								fact.setProductGroupLevelEntName(pglrRow.getEntName());
								fact.setProductGroupTypeName(pglrRow.getTypeName());
								fact.setLevelNo(pglrRow.getLevelNo());
								fact.setLevelName(pglrRow.getLevelName());
								fact.setLevelDesc(pglrRow.getLevelDesc());
							} else {
								fact.unsetProductGroupLevelId();
								fact.setProductGroupLevel1Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevel2Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevel3Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevel4Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevel5Name(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupLevelEntName(DvceConstants.NULL_STRING_VALUE);
								fact.setProductGroupTypeName(DvceConstants.NULL_STRING_VALUE);
								fact.unsetLevelNo();
								fact.setLevelName(DvceConstants.NULL_STRING_VALUE);
								fact.setLevelDesc(DvceConstants.NULL_STRING_VALUE);
							}

							fact.setExtEndCustomerPONo(dsMdfs.getExtEndCustomerPONo());
							fact.setFOBPoint(orderMdfs.getFOBPoint());
							fact.setIsHazardous(dsMdfs.isIsHazardous());
							fact.setPaymentTermsCode(orderMdfs.getPaymentTermsCode());
							if (!FieldUtil.isNull(orderMdfs.getSysPaymentTermsId()))
								fact.setPaymentTermsId(orderMdfs.getSysPaymentTermsId());
							else
								fact.unsetPaymentTermsId();
							fact.setPaymentTermsEnterpriseName(orderMdfs.getPaymentTermsEnterpriseName());
							fact.setDeviationReasonCode(dsMdfs.getDeviationReasonCode());
              fact.setVendorDeviationComment(dsMdfs.getVendorDeviationComment());
              fact.setBuyerCollabReasonCode(dsMdfs.getBuyerCollabReasonCode());
              fact.setBuyerCollabReasonComment(dsMdfs.getBuyerCollabReasonComment());
							if (dsMdfs.isSetPromisePricePer())
								fact.setPromisePricePer(dsMdfs.getPromisePricePer());
							else
								fact.unsetPromisePricePer();
							if (dsMdfs.isSetRequestPricePer())
								fact.setRequestPricePer(dsMdfs.getRequestPricePer());
							else
								fact.unsetRequestPricePer();
							if (dsMdfs.isSetAgreedPricePer())
								fact.setAgreedPricePer(dsMdfs.getAgreedPricePer());
							else
								fact.unsetAgreedPricePer();
							if (dsMdfs.isSetTotalCost())
								fact.setTotalCost(dsMdfs.getTotalCost());
							else
								fact.unsetTotalCost();
							fact.setBackOrderReasonCode(dsMdfs.getBackOrderReasonCode());
							fact.setAutoMoveToReceived(orderMdfs.isAutoMoveToReceived());
							fact.setIncoTermsLocation(orderMdfs.getIncoTermsLocation());
							fact.setASNCreationDate(ds.getASNCreationDate());
							fact.setReceivingWhseStatus(ds.getReceivingWhseStatus());
							fact.setShippingWhseStatus(ds.getShippingWhseStatus());
							fact.setShipmentStatus(ds.getShipmentStatus());
							if (!FieldUtil.isNull(rs.getSysShipToLocationId()))
								fact.setShipToLocationId(rs.getSysShipToLocationId());
							else
								fact.unsetShipToLocationId();
							fact.setShipToLocationName(rs.getShipToLocationName());
							fact.setShipToLocationAddress(rs.getShipToLocationAddress());
							fact.setShipToLocationSiteName(rs.getShipToLocationSiteName());
							if (!FieldUtil.isNull(ds.getSysShipFromLocationId()))
								fact.setShipFromLocationId(ds.getSysShipFromLocationId());
							else
								fact.unsetShipFromLocationId();
							fact.setShipFromLocationName(ds.getShipFromLocationName());
							fact.setShipFromLocationAddress(ds.getShipFromLocationAddress());
							fact.setShipFromLocationSiteName(ds.getShipFromLocationSiteName());
							fact.setExtShipToLocationName(rs.getExtShipToLocationName());
							fact.setExtShipFromLocationName(ds.getExtShipFromLocationName());
							SiteRow shipFromSite = null;
							if (!FieldUtil.isNull(ds.getSysShipFromSiteId())) {
								shipFromSite = SiteCacheManager.getInstance().getSite(ds.getSysShipFromSiteId(),
										vcDvceContext);
							}
							if (shipToSite != null) {
								fact.setShipToSiteId(shipToSite.getSysSiteId());
								fact.setShipToSiteName(shipToSite.getSiteName());
								fact.setShipToDescription(shipToSite.getDescription());
								fact.setShipToTimeZoneId(shipToSite.getTimeZoneId());
							} else {
								fact.unsetShipToSiteId();
								fact.setShipToSiteName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipToTimeZoneId(DvceConstants.NULL_STRING_VALUE);
							}
							if (shipFromSite != null) {
								fact.setShipFromSiteId(shipFromSite.getSysSiteId());
								fact.setShipFromSiteName(shipFromSite.getSiteName());
								fact.setShipFromDescription(shipFromSite.getDescription());
								fact.setShipFromTimeZoneId(shipFromSite.getTimeZoneId());
							} else {
								fact.unsetShipFromSiteId();
								fact.setShipFromSiteName(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromDescription(DvceConstants.NULL_STRING_VALUE);
								fact.setShipFromTimeZoneId(DvceConstants.NULL_STRING_VALUE);
							}
							fact.setExtBPONumber(fact.getExtBPONumber());
							fact.setLnCurrency(line.getCurrency());
							if (order.isSetTotalQuantity())
								fact.setTotalQuantity(order.getTotalQuantity());
							else
								fact.unsetTotalQuantity();
							if (rs.isSetDemandQuantity())
								fact.setDemandQty(rs.getDemandQuantity());
							else
								fact.unsetDemandQty();
							if (rs.isSetOrigRequestQuantity())
								fact.setOrigRequestQty(rs.getOrigRequestQuantity());
							else
								fact.unsetOrigRequestQty();
							fact.setOverrideComment(ds.getOverrideComment());
							if (rs.isSetDemandScaleUpQuantity())
								fact.setDemandScaleUpQuantity(rs.getDemandScaleUpQuantity());
							else
								fact.unsetDemandScaleUpQuantity();
							if (ds.isSetReturnedQuantity())
								fact.setReturnedQuantity(ds.getReturnedQuantity());
							else
								fact.unsetReturnedQuantity();
							if (ds.isSetRejectedQuantity())
								fact.setRejectedQuantity(ds.getRejectedQuantity());
							else
								fact.unsetRejectedQuantity();
							if (rs.isSetProjectedDaysOfSupply())
								fact.setProjectedDaysOfSupply(rs.getProjectedDaysOfSupply());
							else
								fact.unsetProjectedDaysOfSupply();
							if (order.isSetTotalAmount())
								fact.setTotalAmount(order.getTotalAmount());
							else
								fact.unsetTotalAmount();
							fact.setExtVendorItemName(line.getExtVendorItemName());
				              SiteRow consolidationSite = null;
				              if(!FieldUtil.isNull(ds.getSysConsolidationSiteId())) {
				                consolidationSite = SiteCacheManager.getInstance().getSite(ds.getSysConsolidationSiteId(), context);
				                if(consolidationSite != null) {
				                  fact.setConsolidationSiteId(consolidationSite.getSysSiteId());
				                  fact.setConsolidationSiteName(consolidationSite.getSiteName());
				                  fact.setConsolidationSiteDescription(consolidationSite.getDescription());
				                  fact.setConsolidationSiteTimeZoneId(consolidationSite.getTimeZoneId());
				                } else {
				                  fact.unsetConsolidationSiteId();
				                  fact.setConsolidationSiteName(DvceConstants.NULL_STRING_VALUE);
				                  fact.setConsolidationSiteDescription(DvceConstants.NULL_STRING_VALUE);
				                  fact.setConsolidationSiteTimeZoneId(DvceConstants.NULL_STRING_VALUE);
				                }
				              }
				              fact.setConsolidationDeliveryDate(ds.getConsolidationDeliveryDate());
				              fact.setConsolidationPickupDate(ds.getConsolidationPickupDate());
							doOrderFact.add(fact);
						}
					}

				}
			}
			try {
				TransactionSupport.callWithNewTransaction(() -> {
					if (!doOrderFact.isEmpty()) {
						ModelDataServiceUtil.writeModels(DeploymentOrderFact.STANDARD_MODEL_NAME,
								Actions.PLT_INSERT_OR_UPDATE, doOrderFact, vcDvceContext);
					}
					if (!poOrderFact.isEmpty()) {
						ModelDataServiceUtil.writeModels(PurchaseOrderFact.STANDARD_MODEL_NAME,
								Actions.PLT_INSERT_OR_UPDATE, poOrderFact, vcDvceContext);
					}
					if (!soOrderFact.isEmpty()) {
						ModelDataServiceUtil.writeModels(SalesOrderFact.STANDARD_MODEL_NAME,
								Actions.PLT_INSERT_OR_UPDATE, soOrderFact, vcDvceContext);
					}
					return null;
				});
			} catch (Exception e) {
				LOG.error("Error while saving the order fact table ", e);
			}
		}
		return null;
	}
  
  private static Integer getLocalFormatForDate(Calendar cal) {
	  if(cal != null) {
		  Integer day = cal.get(Calendar.DAY_OF_MONTH);
			 Integer year = cal.get(Calendar.YEAR);
			 Integer month =  cal.get(Calendar.MONTH)+1;
			 String lclInString = year.toString() + (month<10?'0'+month.toString():month.toString())
					 +  (day<10?'0'+day.toString():day.toString()) ;
			 return Integer.parseInt(lclInString);
	  }
	 return null;
  }
		  
		  
  
  /**
   * This method used by line level actions, where current model does not have all order lines,
   *  but totals should be calculated considering current order changes, and db order lines
   *
   * @param currentOrders
   * @param currentDBOrders
   */
  public static void copyTotalsToCurrentOrder(List<EnhancedOrder> currentOrders, List<EnhancedOrder> currentDBOrders) {
    for(EnhancedOrder currentOrderDB:currentDBOrders) {
      EnhancedOrder currentOrder = ModelUtil.findMatching(currentOrderDB, currentOrders);
      currentOrder.setTotalQuantity(currentOrderDB.getTotalQuantity());
      currentOrder.setTotalVolume(currentOrderDB.getTotalVolume());
      currentOrder.setTotalAmount(currentOrderDB.getTotalAmount());
      currentOrder.setTotalWeight(currentOrderDB.getTotalWeight());
    }
  }
  
  /**
   * This method used by line level actions, where current model does not have all order lines,
   *  but totals should be calculated considering current order changes, and db order lines
   *
   * @param currentOrders
   * @param currentDBOrders
   */
  public static void copyTotalsToCurrentOrderLine(
    List<EnhancedOrder> currentOrders,
    List<EnhancedOrder> currentDBOrders) {
    for(EnhancedOrder currentOrderDB:currentDBOrders) {
      EnhancedOrder currentOrder = ModelUtil.findMatching(currentOrderDB, currentOrders);
      for (OrderLine line : currentOrder.getOrderLines()) {
        OrderLine lineCurrent = ModelUtil.findMatching(line, currentOrderDB.getOrderLines());
        if(Objects.nonNull(lineCurrent)) {
          line.setLineAmount(lineCurrent.getLineAmount());
        }
      }
    }
  }
  
  /**
   * Method is used specifically when cache order object does not have all order lines but totals should consider all order lines as base lines
   *
   * @param ctx
   * @param order
   */
  public static void getBaseLineForOrderTotalsWithoutCacheAndCalOrderTotal(PlatformUserContext ctx, EnhancedOrder order) {
    List<String> cancelledStates = new ArrayList<String>();
     cancelledStates.add(States.CANCELLED);
     cancelledStates.add(States.DELETED);
     cancelledStates.add(States.VENDOR_REJECTED);
     cancelledStates.add(States.BACKORDERED);
     try {
      List<BaseOrderLineForTotals> baseLines = new ArrayList<BaseOrderLineForTotals>();
      order.setTransientField("vcId", order.getValueChainId());
      order.setTransientField("skipCancelled", true);
      order.setTransientField("cancelledStates", cancelledStates);
      order.setTransientField("useRequestQuantity", true);
      order.setTransientField("ctx", ctx);
      
      baseLines=BaseOrderLinesUtil.getBaseOrderLinesForTotals(order.getValueChainId(), order, true, cancelledStates, true, ctx);
      OrderUtil.updateOrderTotals(order, null, ctx,baseLines);
    }
    catch (VCBaseException e) {
      LOG.error("Error in calculating order totals. ",e);
    }
  }
  
  private  String mapStatus(String state) {
    if (MultiModalShipmentStatusUtil.getClosedStatuses().contains(state)) {
      return com.ordermgmtsystem.supplychaincore.mpt.ShipmentConstants.States.CLOSED;
    }
    else if (MultiModalShipmentStatusUtil.getIntransitStatuses().contains(state)) {
      return com.ordermgmtsystem.supplychaincore.mpt.ShipmentConstants.States.INTRANSIT;
    }else if (MultiModalShipmentStatusUtil.getShippedStatuses().contains(state)) {
      return com.ordermgmtsystem.supplychaincore.mpt.ShipmentConstants.States.INTRANSIT;
    }else if (MultiModalShipmentStatusUtil.getClosedStatuses().contains(state)) {
      return com.ordermgmtsystem.supplychaincore.mpt.ShipmentConstants.States.CLOSED;
    }
    else {
      return com.ordermgmtsystem.supplychaincore.mpt.ShipmentConstants.States.AWAITING;
    }
  }

  public  Set<String> convertStatusToState(Collection<String> statuses) {
    if (CollectionUtils.isEmpty(statuses)) {
      return Collections.emptySet();
        }
    return statuses.stream().map(this::mapStatus).collect(Collectors.toSet());
      }
  
  /**
   * this method copies input order(order) RS to current order(currentDBOrder)
   *
   * @param currentDBOrders this is current order with 
   * @param order this is input order
   * @return
   */
  public static EnhancedOrder copyChildActionRequestSchedule(List<EnhancedOrder> currentDBOrders, EnhancedOrder order) {
    EnhancedOrder currentDBOrder = ModelUtil.findMatching(order, currentDBOrders);
    for(OrderLine line:order.getOrderLines()) {
      OrderLine lineFromDB = ModelUtil.findMatching(line, currentDBOrder.getOrderLines());
      if(lineFromDB!=null) {
        int indexToReplaceOL = currentDBOrder.getOrderLines().indexOf(lineFromDB);
        currentDBOrder.getOrderLines().set(indexToReplaceOL, line);
          }
        }
    return currentDBOrder;
        }
  
  /**
   * this method copies input order(order) RS to current order(currentDBOrder)
   *
   * @param currentDBOrders this is current order with 
   * @param order this is input order
   * @return
   */
  public static EnhancedOrder copyChildActionRequestSchedule(List<EnhancedOrder> currentDBOrders, EnhancedOrder order,boolean copySchedule) {
    if(copySchedule) {
      EnhancedOrder currentDBOrder = ModelUtil.findMatching(order, currentDBOrders);
    for(OrderLine line:order.getOrderLines()) {
        OrderLine lineFromDB = ModelUtil.findMatching(line, currentDBOrder.getOrderLines());
      if(lineFromDB!=null) {
          int indexToReplaceOL = currentDBOrder.getOrderLines().indexOf(lineFromDB);
          for(RequestSchedule reqSch : lineFromDB.getRequestSchedules()) {
            RequestSchedule missingReqSch = ModelUtil.findMatching(reqSch, line.getRequestSchedules());
            if(Objects.isNull(missingReqSch)) {
              line.getRequestSchedules().add(reqSch);
          }
        }
          currentDBOrder.getOrderLines().set(indexToReplaceOL, line);
        }
      }
      return currentDBOrder;
    } else {
      return copyChildActionRequestSchedule(currentDBOrders, order);
    }
    }
  
  /**
   * This method executes totals computation based on input data which is limited as 
   * Child level action contain only part of child models
   * In this case we are trying to find matching object for input in current orders from DB
   * After workflow execution we are changing current DB order with input child data
   * Then executing totals computation for current DB order as it contain all child level objects
   * And input changed data and then copying order header totals and line totals to input order
   * To support those values to be updated in the database after all changes in the workflow 
   *
   * @param inputOrders this is input orders from workflow
   * @param currentDBOrders this is current orders from Database 
   * @param ctx PlatformUserContext
   */
  public static void calculateTotalsForChildLevelActions(
    List<EnhancedOrder> inputOrders,
    List<EnhancedOrder> currentDBOrders,
    PlatformUserContext ctx) {
    for (EnhancedOrder order : inputOrders) {
      EnhancedOrder currentOrder1 = OrderUtil.copyChildActionRequestSchedule(currentDBOrders, order,true);
      OrderUtil.getBaseLineForOrderTotalsWithoutCacheAndCalOrderTotal(ctx, currentOrder1);
      OrderUtil.copyTotalsToCurrentOrderLine(ListUtil.create(order), ListUtil.create(currentOrder1));
      OrderUtil.copyTotalsToCurrentOrder(ListUtil.create(order), ListUtil.create(currentOrder1));
    }
  }
  
  /**
   * This function should create Hold for order for reason codes mentioned in HoldReasonCodes field in csv.
   *
   * @param cleanOrders this is input order
   */
  public static void createOrCloseHoldFromIntegForReasonCodes(List<EnhancedOrder> cleanOrders, String actionName, PlatformUserContext ctx)
    throws Exception {
    final BeanService beanService = Services.get(BeanService.class);
    final HoldService holdService = (HoldService) beanService.getBean(Service.HOLD);  
    UserContextService ucs = Services.get(UserContextService.class);
    PlatformUserContext pltUserContext = ucs.createDefaultValueChainAdminContext(ctx.getValueChainId());
    
    for (EnhancedOrder eo : cleanOrders) {
      List<Hold> holdsInDB = null;
      List<Hold> genericHolds = new ArrayList<Hold>();
      String createHoldReasonCodes = "";
      String closeHoldReasonCodes = "";
      
      if (null != eo.getTransientField(CREATE_HOLD_REASON_CODES)) {
    	holdsInDB = holdService.getHolds(eo, pltUserContext);
        createHoldReasonCodes = (String) eo.getTransientField(CREATE_HOLD_REASON_CODES);
        genericHolds = OrderUtil.createHoldForInteg(eo, createHoldReasonCodes, SCCHoldConstants.States.OPEN, pltUserContext);
        OrderUtil.saveHoldsFromInteg(genericHolds, holdsInDB, eo, pltUserContext);
      }
      
      if (null != eo.getTransientField(CLOSED_HOLD_REASON_CODES)
         && (!FieldUtil.isNull(actionName) && (SCCEnhancedOrderConstants.Actions.UPDATE_FROM_INTEG .equals(actionName))
         || "OMS.CreateOrUpdateFromInteg".equals(actionName))) {
    	if(holdsInDB == null) {
    		holdsInDB = holdService.getHolds(eo, pltUserContext);
    	}
        closeHoldReasonCodes = (String) eo.getTransientField(CLOSED_HOLD_REASON_CODES);
        genericHolds = OrderUtil.createHoldForInteg(eo, closeHoldReasonCodes, SCCHoldConstants.States.CLOSED, pltUserContext);
        OrderUtil.closeHoldsFromInteg(genericHolds, holdsInDB, eo, pltUserContext);
      }
      
    }
  }
  
  /**
   * This function should create Hold for order for reason codes mentioned in HoldReasonCodes field in csv.
   *
   * @param eo this is input order
   * @param reasonCodesFromInteg this is input reason codes
   * @param state this is hold state
   * 
   */
  public static List<Hold> createHoldForInteg(EnhancedOrder eo, String reasonCodesFromInteg, String state, PlatformUserContext pltUserContext)
    throws Exception {

    List<Hold> genericHolds = new ArrayList<Hold>();
    if (null != reasonCodesFromInteg && !FieldUtil.isNull(reasonCodesFromInteg)) {
      String[] holdReasonCodeArray = Arrays.stream(reasonCodesFromInteg.split(",")).distinct().toArray(String[]::new);

      if (holdReasonCodeArray.length > 0
        && (OriginEnum.INTEG.stringValue().equals(eo.getOrigin()) || OriginEnum.EDI.stringValue().equals(eo.getOrigin())
          || OriginEnum.UIUPLOAD.stringValue().equals(eo.getOrigin()))
        && (!newNonTransitionalStates.contains(eo.getState()))) {

        for (String reasonCode : holdReasonCodeArray) {
          LocalizationHelper helper = new LocalizationHelper(LocaleManager.getLocale());
          AbstractOrderValidator pseudoValidator = new AbstractOrderValidator();
          pseudoValidator.setCreateHold(true);
          pseudoValidator.setPlatFormContext((DvceContext) pltUserContext);
          Hold hold = new Hold();
          if (SCCHoldConstants.States.OPEN.equals(state)) {
            hold = pseudoValidator.createHold(
              pltUserContext.getRoleName(),
              reasonCode.trim(),
              helper.getLabel("OMS.Generic.holds.Integ.Description", reasonCode.trim()),
              eo,
              eo.getCreationUser(),
              SCCHoldConstants.States.OPEN,
              eo);
          }
          else if (SCCHoldConstants.States.CLOSED.equals(state)) {
            hold = pseudoValidator.createHold(
              pltUserContext.getRoleName(),
              reasonCode.trim(),
              helper.getLabel("OMS.Generic.holds.Integ.Description", reasonCode.trim()),
              eo,
              eo.getCreationUser(),
              SCCHoldConstants.States.CLOSED,
              eo);
          }

          if (null != hold) {
            hold.unsetBlocking();
            hold.unsetIsStateRollback();
            hold.getMDFs(HoldMDFs.class).setIsManual(true);
          }

          genericHolds.add(hold);
        }

      }
    }

    return genericHolds;

  }

  /**
   * This function should close holds provided as input.
   *
   * @param currentHoldsList input holds
   * @param order input enhanced order
   * @param ctx Platform User Context
   * 
   */
  public static void saveHoldsFromInteg(
    List<Hold> currentHoldsList,
    List<Hold> holdsInDB,
    EnhancedOrder order,
    PlatformUserContext ctx)
    throws Exception {
    
    List<Hold> createHoldsList = null;
    List<Hold> updateHoldsList = null;
    List<Hold> closeHoldsList = null;
    List<Hold> reOpenHoldsList = null;
    List<Hold> draftHoldsList = null;
    
    if(null != currentHoldsList && currentHoldsList.size() > 0) {
      //-- Remove duplicate holds from current holds list from mdf
      List<Hold> uniqueHoldsList = manageHolds.removeDuplicateHolds(currentHoldsList);
      //-- Get the Map for Update and Insert holds lists
      Map<String, List<Hold>> updateAndInsertMap = manageHolds.getHoldsToUpdateAndInsert(uniqueHoldsList, holdsInDB);
      //-- Get the Map for Close and ReOpen holds lists
      Map<String, List<Hold>> closeAndReopenMap = manageHolds.getHoldsToCloseAndReOpen(uniqueHoldsList, holdsInDB);

      //-- Retrieve holds lists from the maps
      if (updateAndInsertMap.containsKey(OrderHoldConstants.CREATE_HOLDS_LIST)) {
        createHoldsList = updateAndInsertMap.get(OrderHoldConstants.CREATE_HOLDS_LIST);
      }
      if (updateAndInsertMap.containsKey(OrderHoldConstants.UPDATE_HOLDS_LIST)) {
        updateHoldsList = updateAndInsertMap.get(OrderHoldConstants.UPDATE_HOLDS_LIST);
      }
      if (closeAndReopenMap.containsKey(OrderHoldConstants.CLOSE_HOLDS_LIST)) {
        closeHoldsList = closeAndReopenMap.get(OrderHoldConstants.CLOSE_HOLDS_LIST);
      }
      if (closeAndReopenMap.containsKey(OrderHoldConstants.REOPEN_HOLDS_LIST)) {
        reOpenHoldsList = closeAndReopenMap.get(OrderHoldConstants.REOPEN_HOLDS_LIST);
      }
      if (updateAndInsertMap.containsKey(OrderHoldConstants.DRAFT_HOLDS_LIST)) {
        draftHoldsList = updateAndInsertMap.get(OrderHoldConstants.DRAFT_HOLDS_LIST);
      }
      
      if (draftHoldsList != null && draftHoldsList.size() > 0) {
        try {
          OrderUtil.setHoldAttributesForOrder(order, draftHoldsList);
          ModelDataService modelDataService = Services.get(ModelDataService.class);
          ModelList<Hold> modelList = new ModelList<Hold>(
            Hold.STANDARD_MODEL_NAME,
            SCCHoldConstants.Actions.CREATE_IN_DRAFT,
            draftHoldsList);
          ModelWriteRequest<Hold> writeRequest = new ModelWriteRequest<Hold>(modelList);
          writeRequest.setRollbackAllOnError(false);
          writeRequest.setReturnProcessedRecords(true);
          modelList = modelDataService.write(writeRequest, ctx);
        }
        catch (Exception e) {
          LOG.info("Error while saving the Hold in Draft State");
        }
      }
      //-- WRITE the holds to DB from above lists
      if (createHoldsList != null && createHoldsList.size() > 0) {
        try {
          OrderUtil.setHoldAttributesForOrder(order, createHoldsList);
          ModelDataService modelDataService = Services.get(ModelDataService.class);
          ModelList<Hold> modelList = new ModelList<Hold>(
            Hold.STANDARD_MODEL_NAME,
            SCCHoldConstants.Actions.CREATE,
            createHoldsList);
          ModelWriteRequest<Hold> writeRequest = new ModelWriteRequest<Hold>(modelList);
          writeRequest.setRollbackAllOnError(false);
          writeRequest.setReturnProcessedRecords(true);
          modelList = modelDataService.write(writeRequest, ctx);
        }
        catch (Exception e) {
          LOG.info("Error while creating the Hold in Draft State");
        }
      }
      
      if (updateHoldsList != null && updateHoldsList.size() > 0) {
        try {
          OrderUtil.setHoldAttributesForOrder(order, updateHoldsList);
          ModelDataService modelDataService = Services.get(ModelDataService.class);
          ModelList<Hold> modelList = new ModelList<Hold>(
            Hold.STANDARD_MODEL_NAME,
            SCCHoldConstants.Actions.UPDATE,
            updateHoldsList);
          ModelWriteRequest<Hold> writeRequest = new ModelWriteRequest<Hold>(modelList);
          writeRequest.setReturnProcessedRecords(true);
          writeRequest.setRollbackAllOnError(false);
          modelList = modelDataService.write(writeRequest, ctx);
        }
        catch (Exception e) {
          LOG.info("Error while updating the Hold in Draft State");
        }
      }
      
      if (reOpenHoldsList != null && reOpenHoldsList.size() > 0) {
        try {
          OrderUtil.setHoldAttributesForOrder(order, reOpenHoldsList);
          ModelDataService modelDataService = Services.get(ModelDataService.class);
          ModelList<Hold> modelList = new ModelList<Hold>(
            Hold.STANDARD_MODEL_NAME,
            SCCHoldConstants.Actions.RE_OPEN,
            reOpenHoldsList);
          ModelWriteRequest<Hold> writeRequest = new ModelWriteRequest<Hold>(modelList);
          writeRequest.setReturnProcessedRecords(true);
          writeRequest.setRollbackAllOnError(false);
          modelList = modelDataService.write(writeRequest, ctx);
        }
        catch (Exception e) {
          LOG.info("Error while re-opening the Hold in Draft State");
        }
      }
      
      if (closeHoldsList != null && closeHoldsList.size() > 0) {
        try {
          OrderUtil.setHoldAttributesForOrder(order, closeHoldsList);
          ModelDataService modelDataService = Services.get(ModelDataService.class);
          ModelList<Hold> modelList = new ModelList<Hold>(
            Hold.STANDARD_MODEL_NAME,
            SCCHoldConstants.Actions.CLOSE,
            closeHoldsList);
          ModelWriteRequest<Hold> writeRequest = new ModelWriteRequest<Hold>(modelList);
          writeRequest.setReturnProcessedRecords(true);
          writeRequest.setRollbackAllOnError(false);
          modelList = modelDataService.write(writeRequest, ctx);
        }
        catch (Exception e) {
          LOG.info("Error while Closing the Hold in Draft State");
        }
      }
      
    }
  }
  
  /**
   * This function should create, update or reopen hold.
   *
   * @param currentHoldsList input holds
   * @param order input enhanced order
   * @param ctx Platform User Context
   * 
   */
  public static void closeHoldsFromInteg(
    List<Hold> inputCloseHoldsList,
    List<Hold> holdsInDB,
    EnhancedOrder order,
    PlatformUserContext ctx)
    throws Exception {
    try {
      if (inputCloseHoldsList != null && inputCloseHoldsList.size() > 0) {
        List<Hold> holdsToClose = new ArrayList<Hold>();
        for (Hold holdInDB : holdsInDB) {
          for (Hold currentHold : inputCloseHoldsList) {
            if (currentHold.getHoldType().equals(holdInDB.getHoldType())
              && currentHold.getHoldReasonCodeReasonCode().equals(holdInDB.getHoldReasonCodeReasonCode())
              && (currentHold.getTransactionId() == holdInDB.getTransactionId())
              && !(holdInDB.getState().equals(SCCHoldConstants.States.OVERRIDDEN)
                || holdInDB.getState().equals(SCCHoldConstants.States.CLOSED))) {
              holdsToClose.add(holdInDB);
              break;
            }
          }
        }
        List<Hold> uniqueHoldsList = manageHolds.removeDuplicateHolds(holdsToClose);
        OrderUtil.setHoldAttributesForOrder(order, uniqueHoldsList);
        ModelDataService modelDataService = Services.get(ModelDataService.class);
        ModelList<Hold> modelList = new ModelList<Hold>(
          Hold.STANDARD_MODEL_NAME,
          SCCHoldConstants.Actions.CLOSE,
          uniqueHoldsList);
        ModelWriteRequest<Hold> writeRequest = new ModelWriteRequest<Hold>(modelList);
        writeRequest.setReturnProcessedRecords(true);
        writeRequest.setRollbackAllOnError(false);
        modelList = modelDataService.write(writeRequest, ctx);
      }
    }
    catch (Exception e) {
      LOG.info("Error while Closing the Hold in Draft State");
    }
  }


  /**
   * TODO complete method documentation
   *
   * @param currentOrder
   */
  public static void clearPriceRelatedFields(EnhancedOrder currentOrder) {
    if(Objects.isNull(currentOrder)) return;
    currentOrder.setTotalAmount(DvceConstants.NULL_DOUBLE_VALUE);
    currentOrder.setCurrency(DvceConstants.NULL_STRING_VALUE);
    currentOrder.setTransientField("isHidePriceInfoFromSupplierEnabled", true);
    for(OrderLine line : currentOrder.getOrderLines()) {
     line.setLineAmount(DvceConstants.NULL_DOUBLE_VALUE);
     line.setUnitPrice(DvceConstants.NULL_DOUBLE_VALUE);
     line.setCurrency(null);
     OrderLineMDFs.from(line).setPricePer(DvceConstants.NULL_DOUBLE_VALUE);
     line.setLineTotalRequestQtyAmount(DvceConstants.NULL_DOUBLE_VALUE);
     line.setLineTotalRequestQtyUOM(null);
     for(RequestSchedule rs : line.getRequestSchedules()) {
       rs.setRsUnitPriceAmount(DvceConstants.NULL_DOUBLE_VALUE);
       rs.setRsUnitPriceUOM(null);
       for(DeliverySchedule ds : rs.getDeliverySchedules()) {
       DeliveryScheduleMDFs.from(ds).setRequestPricePer(DvceConstants.NULL_DOUBLE_VALUE);
       DeliveryScheduleMDFs.from(ds).setPromisePricePer(DvceConstants.NULL_DOUBLE_VALUE);
       DeliveryScheduleMDFs.from(ds).setAgreedPricePer(DvceConstants.NULL_DOUBLE_VALUE);
       ds.setPromiseUnitPriceAmount(DvceConstants.NULL_DOUBLE_VALUE);
       ds.setRequestUnitPriceAmount(DvceConstants.NULL_DOUBLE_VALUE);
       ds.setAgreedUnitPriceAmount(DvceConstants.NULL_DOUBLE_VALUE);
       ds.setFreightCost(DvceConstants.NULL_DOUBLE_VALUE);
       DeliveryScheduleMDFs.from(ds).setTotalCost(DvceConstants.NULL_DOUBLE_VALUE);
       }
     }
    }
  }
  
  /**
   * TODO complete method documentation
   *
   * @param sysId
   * @param sysShipmentLineId
   * @param string
   * @param context
   * @return
   */
  public static ContractNetting getContractNetting(
    Long sysContractLineId,
    Long transactionId,
    String modelLevel,
    PlatformUserContext context) {
    SqlParams sqlParams = new SqlParams();
    sqlParams.setLongValue("SYS_CONTRACT_LINE_ID", sysContractLineId);
    sqlParams.setLongValue("TRANSACTION_ID", transactionId);
    sqlParams.setStringValue("TRANSACTION_MODEL_LEVEL", modelLevel);
    List<ContractNetting> contractNetting = DirectModelAccess.read(
      ContractNetting.class,
      context,
      sqlParams,
      ModelQuery.sqlFilter("SYS_CONTRACT_LINE_ID = $SYS_CONTRACT_LINE_ID$ and TRANSACTION_ID = $TRANSACTION_ID$ and TRANSACTION_MODEL_LEVEL=$TRANSACTION_MODEL_LEVEL$"));
    return !contractNetting.isEmpty() ? contractNetting.get(0) : new ContractNetting();
  }
  
  /**
   * TODO complete method documentation
   *
   * @param sysId
   * @param sysShipmentLineId
   * @param string
   * @param context
   * @return
   */
  public static Map<Pair<Long,Long>,ContractNetting> getContractNetting(
    List<Long> sysContractLineIds,
    List<Long> transactionIds,
    String modelLevel,
    PlatformUserContext context) {
    SqlParams sqlParams = new SqlParams();
    Map<Pair<Long,Long>,ContractNetting> contractNettingMap = new HashMap<Pair<Long,Long>,ContractNetting>();
    sqlParams.setCollectionValue("SYS_CONTRACT_LINE_ID", sysContractLineIds);
    sqlParams.setCollectionValue("TRANSACTION_ID", transactionIds);
    sqlParams.setStringValue("TRANSACTION_MODEL_LEVEL", modelLevel);
    List<ContractNetting> contractNetting = DirectModelAccess.read(
      ContractNetting.class,
      context,
      sqlParams,
      ModelQuery.sqlFilter("SYS_CONTRACT_LINE_ID in $SYS_CONTRACT_LINE_ID$ and TRANSACTION_ID in $TRANSACTION_ID$ and TRANSACTION_MODEL_LEVEL=$TRANSACTION_MODEL_LEVEL$"));
    for(ContractNetting netting:contractNetting) {
      if(netting == null) {
        continue;
      }
      Long contractLineId = netting.getSysContractLineId();
      Long transactionId = netting.getTransactionId();
      Pair<Long,Long> pair = new Pair<Long,Long>(contractLineId, transactionId);
      if(!contractNettingMap.containsKey(pair)) {
        contractNettingMap.put(pair, netting);
      }
      
    }
    return contractNettingMap;
  }
  
  /**
   * TODO complete method documentation
   *
   * @param sysId
   * @param sysShipmentLineId
   * @param string
   * @param context
   * @return
   */
  public static Calendar getResettedDateForNetting(
    Long sysContractLineId,
    PlatformUserContext context) {
    SqlParams sqlParams = new SqlParams();
    sqlParams.setLongValue("SYS_CONTRACT_LINE_ID", sysContractLineId);
    String selectSql ="select max(resetted_date) as resetted_date, sys_contract_line_id  from oms_contract_netting where SYS_CONTRACT_LINE_ID = $SYS_CONTRACT_LINE_ID$ and  IS_RESET = 1  group by sys_contract_line_id ";
    SqlResult result = Services.get(SqlService.class).executeQuery(
    		selectSql,
    		sqlParams);
    if(!result.getRows().isEmpty()) {
    	
    	if(result.getRows().get(0).getValue("resetted_date") !=null ) {
    		Calendar cal = Calendar.getInstance();
    		cal.setTimeInMillis(result.getRows().get(0).getTimestampValue("resetted_date").getTime());
    		return cal;
    	}
    }
    return null;
    
  }
  
  public static void populateEmissionAmount(DeliverySchedule ds,DeliverySchedule currentDS,PlatformUserContext ctx) {
	  
	boolean isPurchaseOrder = ds.getParent().getParent().getParent().getOrderType().equals(OrderTypeEnum.PURCHASE_ORDER.toString()) ? true : false;
    boolean isDeploymentOrder = ds.getParent().getParent().getParent().getOrderType().equals(OrderTypeEnum.DEPLOYMENT_ORDER.toString()) ? true : false;
    Long programId = ds.getParent().getParent().getSysLnProgramId();
    Long itemId = !FieldUtil.isNull(ds.getSysAgreedItemId()) ? ds.getSysAgreedItemId() : ds.getParent().getParent().getSysItemId();
    Long siteId = null;
    Long locationId = null;
    if(isPurchaseOrder || isDeploymentOrder) {
      RequestSchedule reqSchedule =  ds.getParent();
      if(Objects.nonNull(reqSchedule)) {
        siteId = reqSchedule.getSysShipToSiteId();
        locationId = reqSchedule.getSysShipToLocationId();
      }
    } else {
      siteId = ds.getSysShipFromSiteId();
      locationId = ds.getSysShipFromLocationId();
    }
    Pair<Double,String> emissionAmt;
    if (programId != null) {
    	emissionAmt = getEmissionAmount(itemId, siteId, locationId, programId, ctx);
    } else {
    	emissionAmt = getEmissionAmount(itemId, siteId, locationId, ctx);
    }
    if(Objects.nonNull(emissionAmt)) {
      ds.setEmissionAmount(emissionAmt.first*OrderUtil.getQuantityCoalesce(ds));
      ds.setEmissionUOM(WeightUOM._POUND);
    }else {
      ds.setEmissionAmount(DvceConstants.NULL_DOUBLE_VALUE);
      ds.setEmissionUOM(DvceConstants.NULL_STRING_VALUE);
    }
  }
  
  /**
   * TODO complete method documentation
   *
   * @param itemId
   * @param siteId
   * @param locationId
   * @param ctx 
   * @return
   */
  
  public static Pair<Double, String> getEmissionAmount(Long itemId, Long siteId, Long locationId, Long programId, PlatformUserContext ctx) {
	  if (!FieldUtil.isNull(itemId)) {
		  if (!FieldUtil.isNull(siteId)) {
			  Buffer buffer = TransactionCache.getBuffer(itemId, siteId, locationId, programId, ctx);
			  if (Objects.nonNull(buffer)) { 
				  Pair<Double, String> emissionValue = getEmissionAmountForBuffer(buffer, ctx);
				  if (Objects.nonNull(emissionValue)) {
					  return emissionValue;
				  }
			  }
		  }
		  Item item = TransactionCache.getItem(itemId, ctx);
		  if (Objects.nonNull(item)) {
			  return getEmissionAmountForItem(item, ctx);
		  }
	  }
	  return null;
  }
  
  public static Pair<Double, String> getEmissionAmount(Long itemId, Long siteId, Long locationId, PlatformUserContext ctx) {
	if (!FieldUtil.isNull(itemId)) {
      if (!FieldUtil.isNull(siteId)) {
			  Buffer buffer = TransactionCache.getBuffer(itemId, siteId, locationId, -1L, ctx);
        if (Objects.nonNull(buffer)) {
				  Pair<Double, String> emissionValue = getEmissionAmountForBuffer(buffer, ctx);
				  if (Objects.nonNull(emissionValue)) {
					  return emissionValue;
				  }
			  }
		  }
		  Item item = TransactionCache.getItem(itemId, ctx);
		  if (Objects.nonNull(item)) {
			  return getEmissionAmountForItem(item, ctx);
		  }
	  }

	  return null;
  }
  
  public static Pair<Double, String> getEmissionAmountForItem(Item item, PlatformUserContext ctx) {
	  com.ordermgmtsystem.supplychaincore.model.ItemMDFs itemMdf = com.ordermgmtsystem.supplychaincore.model.ItemMDFs
			  .from(item);
	  double emissionAmt = itemMdf.getBuySideEmissionsAmount();
	  String emissionUOM = itemMdf.getBuySideEmissionsUOM();
	  double sellEmissionAmt = itemMdf.getSellSideEmissionsAmount();
	  String sellEmissionUOM = itemMdf.getSellSideEmissionsUOM();
          boolean buySitePresent = false;
          boolean sellSitePresent = false;
          if (!FieldUtil.isNull(emissionAmt) && !FieldUtil.isNull(emissionUOM)) {
        	buySitePresent = true;
            if (!WeightUOM._POUND.toString().equals(emissionUOM)) {
              emissionAmt = convertEmissionAmt(emissionAmt, emissionUOM, WeightUOM._POUND.toString(), ctx);
            }
          }
          if (!FieldUtil.isNull(sellEmissionAmt) && !FieldUtil.isNull(sellEmissionUOM)) {
            sellSitePresent = true;
            if (buySitePresent)
              emissionAmt += convertEmissionAmt(sellEmissionAmt, sellEmissionUOM, WeightUOM._POUND.toString(), ctx);
            else
              emissionAmt = convertEmissionAmt(sellEmissionAmt, sellEmissionUOM, WeightUOM._POUND.toString(), ctx);
          }
          if (sellSitePresent || buySitePresent) {
            return new Pair<>(emissionAmt, WeightUOM._POUND.toString());
	  } else {
		  return null;
          }
        }
  
  public static Pair<Double, String> getEmissionAmountForBuffer(Buffer buffer, PlatformUserContext ctx) {
	  com.ordermgmtsystem.supplychaincore.model.BufferDetailMDFs bufferDetail = com.ordermgmtsystem.supplychaincore.model.BufferDetailMDFs
			  .from(buffer);
	  double emissionAmt = bufferDetail.getBuySideEmissionsAmount();
	  String emissionUOM = bufferDetail.getBuySideEmissionsUOM();
	  double sellEmissionAmt = bufferDetail.getSellSideEmissionsAmount();
	  String sellEmissionUOM = bufferDetail.getSellSideEmissionsUOM();
        boolean buySitePresent = false;
        boolean sellSitePresent = false;
        if (!FieldUtil.isNull(emissionAmt) && !FieldUtil.isNull(emissionUOM)) {
          buySitePresent = true;
          if (!WeightUOM._POUND.toString().equals(emissionUOM)) {
            emissionAmt = convertEmissionAmt(emissionAmt, emissionUOM, WeightUOM._POUND.toString(), ctx);
          }
        }
        if (!FieldUtil.isNull(sellEmissionAmt) && !FieldUtil.isNull(sellEmissionUOM)) {
          sellSitePresent = true;
          if (buySitePresent)
            emissionAmt += convertEmissionAmt(sellEmissionAmt, sellEmissionUOM, WeightUOM._POUND.toString(), ctx);
          else
            emissionAmt = convertEmissionAmt(sellEmissionAmt, sellEmissionUOM, WeightUOM._POUND.toString(), ctx);
        }
        if (sellSitePresent || buySitePresent) {
          return new Pair<>(emissionAmt, WeightUOM._POUND.toString());
	  } else {
		  return null;
    }
  }

  /**
   * TODO complete method documentation
   *
   * @param order
   * @param currentOrder 
   * @param ctx
   */
  public static void calculateTotalEmissionAmount(EnhancedOrder order, EnhancedOrder currentOrder, PlatformUserContext ctx) {
    double totalEmissionAmt = NullConstants.NULL_DOUBLE_VALUE;
    for (OrderLine line : order.getOrderLines()) {
      for (RequestSchedule reqSchedule : line.getRequestSchedules()) {
        for (DeliverySchedule delSchedule : reqSchedule.getDeliverySchedules()) {
          if(!FieldUtil.isNull(delSchedule.getEmissionAmount()) && 
            !OrderUtil.nonTransitionalStates.contains(delSchedule.getState())) {
            double emissionAmt = delSchedule.getEmissionAmount();
            if(!FieldUtil.isNull(emissionAmt) && !FieldUtil.isNull(delSchedule.getEmissionUOM()) &&
              !WeightUOM._POUND.toString().equals(delSchedule.getEmissionUOM())) {
            	emissionAmt = convertEmissionAmt( emissionAmt, 
                            delSchedule.getEmissionUOM(), WeightUOM._POUND.toString(), ctx);
            }
            if(!FieldUtil.isNull(totalEmissionAmt)) {
              totalEmissionAmt += emissionAmt;
            } else {
              totalEmissionAmt = emissionAmt;
            }
          }
        }
      }
    }
    order.setTotalEmissionAmount(totalEmissionAmt);
    order.setTotalEmissionUOM(WeightUOM._POUND.toString());
  }

  /**
   * TODO complete method documentation
   *
   * @param filter
   * @param primarySite 
   */
	public static FilterJSONObject setPrimarySite(FilterJSONObject filter, SiteRow primarySite) {
		if (primarySite != null && filter != null) {
			filter.put("value", primarySite.getSysSiteId());
			if (primarySite.getDescription() != null) {
				filter.put("displayValue", primarySite.getSiteName() + " - " + primarySite.getDescription());
			} else {
				filter.put("displayValue", primarySite.getSiteName() + " - ");
			}
		}
		return filter;
	}
  
  private static double convertEmissionAmt(double value, String valueUOM, String targetUOM, PlatformUserContext ctx) {
	  double emissionAmt = value;
	  try {
		  emissionAmt = uomService.convert("WeightUOM", value, valueUOM, targetUOM, ctx);
	  } catch(UndefinedConversionFactorException e) {
			  throw new UndefinedConversionFactorException("Cannot convert emission amount to " + WeightUOM._POUND.toString() + 
					  ". Conversion factor is not defined for UoM: " + valueUOM + ".");
	  }
	  
	  return emissionAmt;
  }
  
  public static void createAttachedTransientNotes(List<EnhancedOrder> orders, PlatformUserContext context) {
    List<Note> notes = new ArrayList<>();
    for(var order : orders) {
      @SuppressWarnings("unchecked")
      List<Note> orderNotes = (List<Note>)order.getTransientField("POST_WRITE_NOTES");
      if(orderNotes == null) continue;
      for(var note : orderNotes) {
        note.setOwnerId(order.getSysId());
        notes.add(note);
      }
    }
    if(!notes.isEmpty()) {
      ModelList<Note> modelList = new ModelList<>(SCCNoteConstants.Actions.CREATE, notes);
      ModelDataService mds = Services.get(ModelDataService.class);
      mds.write(modelList, context);
    }
  }
  
  public static List<String> getExpiryThreshold(List<Organization> o){
    Integer value = TransactionCache.getOrgPolicy(
      OMSConstants.Policies.CONTRACT_TERM_EXPIRING_SOON_THRESHOLD,
     -1L,
      14,
      null);
    if(value!=null  ) {
      return OPW_RESTRICTED_STATES;
    } else {
      return OPW_STATES;
    }
  }
  
  public static List<EnhancedOrder> filterOutErrorModels(List<EnhancedOrder> orders, String actionName) {
    boolean isPartialRollbackEnabled = ExternalRefUtil.isPartialOrderRollbackEnabled(actionName);
    if(isPartialRollbackEnabled) {
      return orders.stream()
        .filter(order -> order.getError() == null)
        .collect(toList());
    } else {
      return ModelHelper.filterOutErrorModels(orders);
    }
  }
  
  public static String getCurrentActionName() {
    return RequestScope.getInstance().getWorkflowContext().getActionName();
  }
  
  public static void clearBuyerCollabReasonCode(EnhancedOrder order, String vendorChangeRequested) {
    for(OrderLine line : order.getOrderLines()) {
      for(RequestSchedule rs :  line.getRequestSchedules()) {
        for(DeliverySchedule ds : rs.getDeliverySchedules()) {
          if(vendorChangeRequested.equalsIgnoreCase(ds.getState())) {
            ds.getMDFs(DeliveryScheduleMDFs.class).setBuyerCollabReasonCode(DvceConstants.NULL_STRING_VALUE);
            ds.getMDFs(DeliveryScheduleMDFs.class).setBuyerCollabReasonComment(DvceConstants.NULL_STRING_VALUE);
          }
        }
      }
    }
  }
  
  
  public static ValuesForForceCloseBackorderEnum getPolicyDefaultShippedAndReceivedValuesForForceCloseBackorder(
    EnhancedOrder order
    , PlatformUserContext context) {
    
    String policyValue = TransactionCache.getOrgPolicy(
      OMSConstants.Policies.DEFAULT_SHIPPED_AND_RECEIVED_VALUES_FOR_FORCE_CLOSE_BACKORDER,
      order.getSysOwningOrgId(),
      ValuesForForceCloseBackorderEnum.BLANK.stringValue(),
      context);
    
    ValuesForForceCloseBackorderEnum policyEnum = ValuesForForceCloseBackorderEnum.get(policyValue);
    return policyEnum;
  }
  
  public static Boolean getBooleanPolicyValue(String policyName, Long id ,ModelLevelType type,
		  Boolean defaultValue, PlatformUserContext platformUserContext ) {
	  boolean collabPerDS =  Services.get(PolicyService.class).getPolicyValue(
			  policyName, type,  id, defaultValue,  platformUserContext);
	  return collabPerDS;
  }
  
  public static String getOrderingUOM(ItemRow itemRow, BufferRow bufferRow, Integer defaultUOM) {
	  Integer uom = null;
	  EnumerationType uomName = EnumerationType.QUANTITY_UOM;
	  if(bufferRow!=null) {
		  uom = bufferRow.getOrderUom();		
	  } 
	  if(FieldUtil.isNull(uom) && itemRow!=null && !FieldUtil.isNull(itemRow.getOrderUom())) {
		  uom = itemRow.getOrderUom();
	  } 
	  if(FieldUtil.isNull(uom) && bufferRow!=null && !FieldUtil.isNull(bufferRow.getOrderingUom())) {
		  uom = bufferRow.getOrderingUom();
		  uomName = EnumerationType.ORDERING_UOM;
	  } 
	  if(FieldUtil.isNull(uom) && itemRow!=null && !FieldUtil.isNull(itemRow.getOrderingUom())) {
		  uom = itemRow.getOrderingUom();
		  uomName = EnumerationType.ORDERING_UOM;
	  }
	  if(FieldUtil.isNull(uom) ){
		  uom = defaultUOM;
		  LOG.info("No UOM found , setting as defaultUOM");
	  }
	  if(!FieldUtil.isNull(uom)) {
		return  OMSUtil.getEnumStr(uomName, uom);
	  }
	  return null;
	  
  }
  
  
  /**
   * Use this method to perform ds level collaboration actions from Integ
   *
   * @param inputOrders
   * @param currentOrders
   * @param dsActionName
   * @param ctx
   */
  public static void performDsActionOrdersForInteg(List<EnhancedOrder> inputOrders,  List<EnhancedOrder> currentOrders, String dsActionName, PlatformUserContext ctx){
    List<EnhancedOrder> actionOrders = new ArrayList<EnhancedOrder>();
    List<EnhancedOrder> inputActionOrders = new ArrayList<EnhancedOrder>();
    List<EnhancedOrder> currentActionOrders = new ArrayList<EnhancedOrder>();
    
    for(EnhancedOrder eo : inputOrders) {
      EnhancedOrder clonnedOrderForInput = OrderUtil.getClonedOrder(eo);
      inputActionOrders.add(clonnedOrderForInput);
    }
    
    for(EnhancedOrder eo : currentOrders) {
      EnhancedOrder clonnedOrderForInput = OrderUtil.getClonedOrder(eo);
      currentActionOrders.add(clonnedOrderForInput);
    }
    
    PlatformUserContext adminContext = ctx;
    for(EnhancedOrder inputOrder : inputActionOrders) {
      adminContext = OrderUtil.getEntAdminContext(inputOrder, ctx);
      EnhancedOrder currentOrder = ModelUtil.findMatching(inputOrder, currentActionOrders);
      
      if(OrderUtil.vendorCollabActionsForInteg.contains(dsActionName))
        inputOrder.getOrderLines().forEach(orderLine1 -> orderLine1.getRequestSchedules()
          .forEach(requestSchedule1 -> requestSchedule1.getDeliverySchedules()
            .removeIf(deliverySchedule -> 
              !FieldUtil.isNull((String) deliverySchedule.getParent().getTransientField("ChildLevelAction"))
                && !((String) deliverySchedule.getParent().getTransientField("ChildLevelAction"))
                .equalsIgnoreCase(dsActionName))));
      
      if(OrderUtil.buyerCollabActionsForInteg.contains(dsActionName)) 
        inputOrder.getOrderLines().forEach(orderLine1 -> orderLine1.getRequestSchedules()
          .forEach(requestSchedule1 -> requestSchedule1.getDeliverySchedules()
            .removeIf(deliverySchedule -> 
              !FieldUtil.isNull((String) deliverySchedule.getParent().getTransientField("RequestScheduleAction"))
                && !((String) deliverySchedule.getParent().getTransientField("RequestScheduleAction"))
                .equalsIgnoreCase(dsActionName))));
      
      
      inputOrder.getOrderLines().forEach(orderLine1 -> orderLine1.getRequestSchedules()
        .removeIf(requestSchedule1 -> Objects.nonNull(requestSchedule1.getTransientField("ExecutedAction"))));
      
      inputOrder.getOrderLines().forEach(orderLine1 -> orderLine1.getRequestSchedules()
        .removeIf(requestSchedule1 -> requestSchedule1.getDeliverySchedules().isEmpty()));
      
      inputOrder.getOrderLines()
        .removeIf(orderLine1 -> orderLine1.getRequestSchedules().isEmpty());
      
      if(!inputOrder.getOrderLines().isEmpty()) {
        inputOrder.setActionName(null);
        actionOrders.add(inputOrder);
      }
    }
    
    if (actionOrders.size() > 0) {
      ModelList<EnhancedOrder> eoModelList = ModelDataServiceUtil.writeModels(
        EnhancedOrder.STANDARD_MODEL_NAME,
        dsActionName,
        actionOrders,
        adminContext);
      copyErrors(eoModelList.getModels(), inputOrders);
    }
    
  }
  
  public static void copyErrors(List <EnhancedOrder> sourceOrderList , List<EnhancedOrder> targetOrderList) {
    for (EnhancedOrder eo : sourceOrderList) {
      for (EnhancedOrder io : targetOrderList) {
        if (io.getOrderNumber().equals(eo.getOrderNumber())) {
          io.setError(eo.getError());
          for (OrderLine ol : eo.getOrderLines()) {
            for (OrderLine iol : io.getOrderLines()) {
              if (iol.getLineNumber().equals(ol.getLineNumber())) {
                iol.setError(ol.getError());
                for (RequestSchedule rs : ol.getRequestSchedules()) {
                  for (RequestSchedule irs : iol.getRequestSchedules()) {
                    if (irs.getRequestScheduleNumber().equals(rs.getRequestScheduleNumber())) {
                      irs.setError(rs.getError());
                      for (DeliverySchedule ds : rs.getDeliverySchedules()) {
                        for (DeliverySchedule ids : irs.getDeliverySchedules()) {
                          if (ids.getDeliveryScheduleNumber().equals(ds.getDeliveryScheduleNumber())) {
                            ids.setError(ds.getError());
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  public static boolean validateUserSiteAssoc(List<Long> siteIds, DvceContext dvceCtx) {
    boolean allSitesAssoced = true;
    //Collect site assoc
    if (siteIds != null) {
      SqlParams sqlParams = new SqlParams();
      sqlParams.setCollectionValue("SITE_IDS", siteIds);
      List<UserSiteAssoc> userSiteAssocList = DirectModelAccess.read(
        UserSiteAssoc.class,
        dvceCtx,
        sqlParams,
        ModelQuery.sqlFilter("SYS_SITE_ID in $SITE_IDS$"));

     
      boolean isUserAssocToSite = true;
      for (Long siteToCheck : siteIds) {
        List<UserSiteAssoc> siteAssocedToUsers = userSiteAssocList.stream().filter(
          usa -> usa.getSysSiteId().equals(siteToCheck)).collect(Collectors.toList());
        if (Objects.nonNull(siteAssocedToUsers) && !siteAssocedToUsers.isEmpty()) {
          isUserAssocToSite = siteAssocedToUsers.stream().anyMatch(
            usa -> usa.getSysUserId().equals(dvceCtx.getUserId()));
          if (!isUserAssocToSite) {
            allSitesAssoced = false;
            break;
          }
        }
      }
    }
    return allSitesAssoced;
  }

  /**
   * TODO complete method documentation
   *
   * @param req
   * @return
   */
  public static boolean isVMICheckDisabledInContractSourcing(Long sysOrgID) {
    if (sysOrgID != null && sysOrgID < 1)
      return false;
    
    OrganizationRow contractOrgRow =  OrganizationCacheManager.getInstance().getOrganization(sysOrgID);
    boolean isVMICheckDisabled = Boolean.valueOf(ExternalReferenceUtil.getLocalValue(
      ContractConstants.DISABLE_VMI_CHECK_IN_CONTRACT_SOURCING,
      contractOrgRow.getEntName()));
    return isVMICheckDisabled;
  }
  
  public static String getOrderStateLabel(String state, String orderType) {
    String stateLabel = state;
    if(OrderTypeEnum.DEPLOYMENT_ORDER.toString().equals(orderType)) {
      stateLabel = LabelUtil.getMessage(
        "meta.field.State.SCC."+orderType.replace(" ", "")+ "."+ state,
        LocaleManager.getLocale(),
        MessageBundleFactory.getMessageBundle(),
        state);
    } else {
      stateLabel = LabelUtil.getMessage(
        "meta.field.State.SCC.EnhancedOrder."+ state,
        LocaleManager.getLocale(),
        MessageBundleFactory.getMessageBundle(),
        state);
    }
    return stateLabel;
  }
  
  public static Map<RequestSchedule, Item> getItemPerRS(EnhancedOrder order, PlatformUserContext ctx) {
    try (PSRLoggerEntry psrLogger = new PSRLoggerEntry(PSR_ID, OrderUtil.class, "checkInactiveItem")) {
      Map<RequestSchedule,Item> reqSchPerItem = new HashMap<>();
      for (OrderLine line : order.getOrderLines()) {
        if(!FieldUtil.isNull(line.getSysItemId())) {
          Item currentItem = TransactionCache.getItem(line.getSysItemId(), ctx);
          if(Objects.nonNull(currentItem)) {
            for(RequestSchedule rs  : line.getRequestSchedules()) {
              reqSchPerItem.put(rs, currentItem);
            }
          }
        }
      }
      return reqSchPerItem;
    }
  }

  public static void autopopulateValuefromOtherTransaction(
    EnhancedOrder targetOrder,
    EnhancedOrder dbOrder,
    PlatformUserContext ctx) {
    if(!OrderUtil.nonTransitionalStates.contains(targetOrder.getState())) {
      OrganizationRow org = OrganizationCacheManager.getInstance().getOrganization(targetOrder.getSysOwningOrgId());
      List<TransFieldMapping> transFieldMappingRules = TransactionFieldMappingScanner.getInstance().
        getTransModelMappingRules(targetOrder.getModelType().toString(), targetOrder.getModelType().toString(), 
          org.getSysEntId(), org.getSysOrgId(),targetOrder.getOrderType(), ctx);
      TransactionFieldMappingScanner.getInstance().copyValueFromOneTransToOtherTrans(targetOrder.getModelLevelType().toString(),
        dbOrder, targetOrder, null, targetOrder.getSysOwningOrgId(), transFieldMappingRules,OrderRestUtil.getOrderType(targetOrder), ctx);
    }
  }

  /**
   * TODO complete method documentation
   *
   * @param sourceModel
   * @param sourceModelLevelType
   * @param ctx 
   * @return
   */
  public static Map<RequestSchedule,? extends Model> getDesiredModelList(EnhancedOrder order, String sourceModelLevelType, PlatformUserContext ctx) {
   switch(sourceModelLevelType){
     case "SCC.AvlLine":
       return  OrderUtil.getAVLLineFromRS(OrderUtil.getAllRequestSchedules(order), ctx);
     case "Buffer":
       return  OrderUtil.getBufferPerRS(ListUtil.create(order), ctx);
     case "Item":
       return OrderUtil.getItemPerRS(order, ctx);
     case "BufferDetail":
       return  OrderUtil.getBufferPerRS(ListUtil.create(order), ctx);
   }
    return null;
  }
  
  public static String convertToProfileNumberFormat(Double number, String numberFormat ) {
    if(number==null)
      return "0";
    try {
      DecimalFormat twoDForm = new DecimalFormat(numberFormat);
      return twoDForm.format(number).replaceAll(",", "");
    }catch(Exception e) {
      NumberFormat numFormat = NumberFormat.getInstance(Locale.getDefault());
      return numFormat.format(number);
    }
  }
  
  public static void reinstateSchedule(RequestSchedule rs, PlatformUserContext context) {
    String newState = getReinstateState(rs);
    rs.setState(newState);
    for(DeliverySchedule ds : rs.getDeliverySchedules()) {
      if(States.DELETED.equals(ds.getState()) || States.CANCELLED.equals(ds.getState())) {
        ds.setState(newState);
        clearCollaborationFields(ds);
      }
    }
  }
  
  public static void clearCollaborationFields(DeliverySchedule ds) {
    clearAgreedFields(ds, States.CANCELLED);
    clearPromiseFields(ds, States.CANCELLED);
  }
  
  private static String getReinstateState(RequestSchedule rs) {
    String headerState = rs.getParent().getParent().getState();
    switch(headerState) {
      case com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.DRAFT:
      case States.AWAITING_APPROVAL:
      case States.CANCELLED:
      case States.DELETED:
        return com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.AWAITING_APPROVAL;
      case States.NEW:
      case com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.IN_PROMISING:
        return States.NEW;
    }
    return States.BUYER_CHANGE_REQUESTED;
  }
  public static List<OrderLine> getRelaventChildLines(OrderLine parentOL, List<OrderLine> childOL) {
    Set<OrderLine> relaventChildLines = new HashSet<>();
    Long parentOrderLineId = parentOL.getSysId();
    for(OrderLine childOrderLine : childOL) {
      if(!FieldUtil.isNull(childOrderLine.getSysParentOrderLineId())) {
        if(parentOrderLineId.equals(childOrderLine.getSysParentOrderLineId())) {
          relaventChildLines.add(childOrderLine);
        }
      }else {
        Long itemId = parentOL.getSysItemId();
        String extItemName = parentOL.getExtItemName();
        String childItemName = childOrderLine.getExtItemName();
        Long childItemId = childOrderLine.getSysItemId();
        String buyerEnt = null;
        String mappedItem = null;
        if(!FieldUtil.isNull(itemId) && !FieldUtil.isNull(childItemId)) {
          if(parentOL!=null && parentOL.getParent()!=null )
            buyerEnt = OrganizationCacheManager.getInstance().getOrganization(parentOL.getParent().getSysBuyingOrgId()).getEntName();
          if(buyerEnt!=null)
            mappedItem =OrderUtil.getMappedItem(childItemId, buyerEnt);
          boolean isItemMatching =itemId.equals(childItemId);//parent and child item should match
          boolean isMappedItemMatching=false;
          if(mappedItem!=null)
            isMappedItemMatching=itemId.equals(Long.valueOf(mappedItem)); //parent and mapped item should match
          if(isItemMatching || isMappedItemMatching) { 
            relaventChildLines.add(childOrderLine);
          }
        }else if(!FieldUtil.isNull(extItemName) && !FieldUtil.isNull(childItemName)) {
          if(childItemName.equals(extItemName)) {
            relaventChildLines.add(childOrderLine);
          }
        }
      }
    }
    return  relaventChildLines.stream().collect(Collectors.toList());
  }
  
  public static List<RequestSchedule> getRelaventChildRS(RequestSchedule parentRS, List<RequestSchedule> childRS) {
    Set<RequestSchedule> relaventRS = new HashSet<>();
      Long parentRSId = parentRS.getSysId();
      for(RequestSchedule childReqSch : childRS) {
        RequestScheduleMDFs reqSchMDF = childReqSch.getMDFs(RequestScheduleMDFs.class);
        if(!FieldUtil.isNull(reqSchMDF.getSysParentReqScheduleId()) && parentRSId.equals(reqSchMDF.getSysParentReqScheduleId())) {
          relaventRS.add(childReqSch);
        }else {
          relaventRS.addAll(getRelaventRSByMatchingItem(parentRS, ListUtil.create(childReqSch)));
        }
      }
     
      return  relaventRS.stream().collect(Collectors.toList());
  }
  
  public static List<RequestSchedule> getRelaventRSByMatchingItem(RequestSchedule parentRS, List<RequestSchedule> childRS) {
    Set<RequestSchedule> relaventRS = new HashSet<>();
    Long parentItemId = parentRS.getParent().getSysItemId();
    Long shipFromSiteId = !FieldUtil.isNull(parentRS.getDeliverySchedules().get(0).getSysShipFromSiteId()) ? 
        parentRS.getDeliverySchedules().get(0).getSysShipFromSiteId() : parentRS.getSysShipToSiteId();
    if(!FieldUtil.isNull(parentItemId)) {
      for(RequestSchedule childReqSch : childRS) {
        RequestScheduleMDFs reqSchMDF = childReqSch.getMDFs(RequestScheduleMDFs.class);
        if(!FieldUtil.isNull(reqSchMDF.getSysParentReqScheduleId())) 
          continue;
        Long childItemId = childReqSch.getParent().getSysItemId();
        String sellerEnt = null;
        String mappedItem = null;
        if(parentRS.getParent()!=null && parentRS.getParent().getParent()!=null ) {
          OrganizationRow sellingOrg= OrganizationCacheManager.getInstance().getOrganization(parentRS.getParent().getParent().getSysSellingOrgId());
          if(sellingOrg!=null)
            sellerEnt = sellingOrg.getEntName();
        }
         
        if(sellerEnt!=null && !FieldUtil.isNull(childItemId))
          mappedItem =OrderUtil.getMappedItem(parentItemId, sellerEnt);
        boolean isItemMatching =parentItemId.equals(childItemId);
        boolean isMappedItemMatching=false;
        if(mappedItem!=null)
          isMappedItemMatching=childItemId.equals(Long.valueOf(mappedItem));
        if(!FieldUtil.isNull(childItemId) &&( isItemMatching||  isMappedItemMatching)) { 
          if(childReqSch.getParent().isDropShipment() && parentRS.getSysShipToSiteId().equals(childReqSch.getSysShipToSiteId())) {
            relaventRS.add(childReqSch);
          }else if(shipFromSiteId.equals(childReqSch.getSysShipToSiteId())){
            relaventRS.add(childReqSch);
          }
        }
      }
    }else {
      String itemName =parentRS.getParent().getExtItemName();
      if(!FieldUtil.isNull(itemName)) {
        for(RequestSchedule childReqSch : childRS) {
          RequestScheduleMDFs reqSchMDF = childReqSch.getMDFs(RequestScheduleMDFs.class);
          if(!FieldUtil.isNull(reqSchMDF.getSysParentReqScheduleId())) 
            continue;
          String childItemName = childReqSch.getParent().getExtItemName();
          if(!FieldUtil.isNull(childItemName) && 
              childItemName.equals(itemName)) {
            if(childReqSch.getParent().isDropShipment() && parentRS.getSysShipToSiteId().equals(childReqSch.getSysShipToSiteId())) {
              relaventRS.add(childReqSch);
            }else if(shipFromSiteId.equals(childReqSch.getSysShipToSiteId())){
              relaventRS.add(childReqSch);
            }
          }
        }
      }
    }
    return relaventRS.stream().collect(Collectors.toList());
}

  /**
   * TODO complete method documentation
   *
   * @param model
   * @param orderType 
   * @param platformUserContext 
   * @return
   */
  public static List<EnhancedOrder> getExistingChildOrders(
    EnhancedOrder model,
    String orderType, PlatformUserContext platformUserContext) {
    List<String> deadStates = ListUtil.create(States.CANCELLED, States.VENDOR_REJECTED, States.DELETED);
    deadStates.addAll(inFullFillmentStates);
    SqlParams params = new SqlParams();
    params.setLongValue("SYS_PARENT_ORDER_ID", model.getSysId());
    if(Objects.nonNull(orderType)) {
      params.setStringValue("ORDER_TYPE", orderType);
    }
    params.setCollectionValue("STATE", deadStates);
    List<EnhancedOrder> childOrders = new ArrayList<>();
    childOrders = DirectModelAccess.read(
      EnhancedOrder.class,
      platformUserContext,
      params,
      ModelQuery.sqlFilter(
        "SYS_PARENT_ORDER_ID in $SYS_PARENT_ORDER_ID$ AND STATE NOT IN $STATE$ AND ORDER_TYPE IN $ORDER_TYPE$"));
    return childOrders;
  }

  /**
   * TODO complete method documentation
   *
   * @param updateChildOrders
   * @param updateCO 
   * @param sourceModel 
   * @param platformUserContext
   * @return 
   */
  public static void updateRelaventChildOrders(
    List<EnhancedOrder> updateChildOrders,
    boolean updateCO,
    EnhancedOrder model,
    PlatformUserContext platformUserContext) {
    UserContextService contextService = Services.get(UserContextService.class);
    PlatformUserContext vcAdminCtx = contextService.createDefaultValueChainAdminContext(
      platformUserContext.getValueChainId());
    updateChildOrders = updateChildOrders.stream().distinct().collect(Collectors.toList());
    if (updateCO) {
      if (Objects.nonNull(updateChildOrders)) {
        List<EnhancedOrder> childOrdersAwaiting = updateChildOrders.stream().filter(
          order -> (order.getState().equalsIgnoreCase(States.AWAITING_APPROVAL) || order.getState().equalsIgnoreCase(
            com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.DRAFT))).collect(Collectors.toList());
        List<EnhancedOrder> childOrdersNEW = updateChildOrders.stream().filter(
          order -> (order.getState().equalsIgnoreCase(States.NEW))).collect(Collectors.toList());
        List<EnhancedOrder> childOrdersPostNEW = updateChildOrders.stream().filter(
          order -> (order.getState().equalsIgnoreCase(States.OPEN)
            || order.getState().equalsIgnoreCase(States.BUYER_CHANGE_REQUESTED))).collect(Collectors.toList());

        ModelList<EnhancedOrder> poAAModelList = null, poNewModelList = null, poPostNEWModelList = null,
          poSyncModelList = null;
        if (childOrdersAwaiting.size() > 0)
          poAAModelList = ModelDataServiceUtil.writeModels(
            EnhancedOrder.STANDARD_MODEL_NAME,
            SCCEnhancedOrderConstants.Actions.UPDATE,
            childOrdersAwaiting,
            vcAdminCtx);
        if (null != poAAModelList && !poAAModelList.getErrors().isEmpty()) {
          model.setError("Error while fulfilling the order with order type(purchase order) "+poAAModelList.getErrors().toString());
        }
        if (childOrdersNEW.size() > 0)
          poNewModelList = ModelDataServiceUtil.writeModels(
            EnhancedOrder.STANDARD_MODEL_NAME,
            SCCEnhancedOrderConstants.Actions.REVISE,
            childOrdersNEW,
            vcAdminCtx);
        if (null != poNewModelList && !poNewModelList.getErrors().isEmpty()) {
          model.setError("Error while fulfilling the order with order type(purchase order) "+poNewModelList.getErrors().toString());
        }
        if (childOrdersPostNEW.size() > 0)
          poPostNEWModelList = ModelDataServiceUtil.writeModels(
            EnhancedOrder.STANDARD_MODEL_NAME,
            SCCEnhancedOrderConstants.Actions.BUYER_CHANGE_REQUEST,
            childOrdersPostNEW,
            vcAdminCtx);
        if (null != poPostNEWModelList && !poPostNEWModelList.getErrors().isEmpty()) {
          model.setError("Error while fulfilling the order with order type(purchase order) "+poPostNEWModelList.getErrors().toString());
        }
        List<EnhancedOrder> childOrdersForSynchPO = updateChildOrders.stream().filter(
          order -> (order.getState().equalsIgnoreCase(States.VENDOR_CHANGE_REQUESTED)
            || order.getState().equalsIgnoreCase(States.VENDOR_CONFIRMED_WITH_CHANGES)
            || order.getState().equalsIgnoreCase(States.BUYER_CONFIRMED_WITH_CHANGES))).collect(Collectors.toList());

        if (childOrdersForSynchPO.size() > 0)
          for (EnhancedOrder childOrder : childOrdersForSynchPO) {
            for (OrderLine ol : childOrder.getOrderLines()) {
              for (RequestSchedule rs : ol.getRequestSchedules()) {
                //                if (Objects.nonNull(rs.getTransientField("revisitState")) && rs.getTransientField("revisitState").equals(true)) {}
                if (!Objects.nonNull(rs.getState())) {
                  for (DeliverySchedule ds : rs.getDeliverySchedules())
                    ds.setState(States.BUYER_CHANGE_REQUESTED);
                  rs.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(rs)));
                }
              }
              ol.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(ol)));
            }
            childOrder.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(childOrder)));
          }
        poSyncModelList = ModelDataServiceUtil.writeModels(
          EnhancedOrder.STANDARD_MODEL_NAME,
          SCCEnhancedOrderConstants.Actions.SYNCH_PO,
          childOrdersForSynchPO,
          vcAdminCtx);
        if (null != poSyncModelList && !poSyncModelList.getErrors().isEmpty()) {
          model.setError("Error while fulfilling the order with order type(purchase order) "+poSyncModelList.getErrors().toString());

        }
      }
    }
  }
  

  /**
   * TODO complete method documentation
   *
   * @param parentRS
   * @param childReqSchs
   * @return 
   */
  public static double getRemainingQtyFromParentRS(RequestSchedule parentRS, List<RequestSchedule> childReqSchs,PlatformUserContext context) {
	EnhancedOrder parentOrder = parentRS.getParent().getParent();
    List<DeliverySchedule> deliverySchedules = parentRS.getDeliverySchedules();
    boolean collabPerDS = Services.get(PolicyService.class).getPolicyValue(
            OMSConstants.Policies.ENABLE_BUYER_COLLABORATION_PER_DS, Organization.MODEL_LEVEL_TYPE,  parentOrder.getSysOwningOrgId(), false, context);
    double totalParentRQ = parentRS.getDeliverySchedules().get(0).getRequestQuantity();
    if(collabPerDS) {
    	totalParentRQ = deliverySchedules.stream().filter(
    			ds -> !OrderUtil.nonTransitionalStates.contains(ds.getState())).mapToDouble(
    					DeliverySchedule::getRequestQuantity).sum();
    }
    double sumOfAllCRS = 0;
    Map<Long,Boolean> childOrdersPerDS = new HashMap<>();
    for(RequestSchedule childReqSch : childReqSchs) {
    	Long owningOrgId = childReqSch.getParent().getParent().getSysOwningOrgId();
    	if(Objects.isNull(childOrdersPerDS.get(owningOrgId))) {
    		boolean collabDS = Services.get(PolicyService.class).getPolicyValue(
    	            OMSConstants.Policies.ENABLE_BUYER_COLLABORATION_PER_DS, Organization.MODEL_LEVEL_TYPE,  owningOrgId, false, context);
    		childOrdersPerDS.put(owningOrgId, collabDS);
    	}
    	if(childOrdersPerDS.get(owningOrgId)) {
    		sumOfAllCRS += childReqSch.getDeliverySchedules().stream().filter(
    			        ds -> !OrderUtil.nonTransitionalStates.contains(ds.getState())).mapToDouble(
    			          DeliverySchedule::getRequestQuantity).sum();
    	} else {
    		sumOfAllCRS += childReqSch.getDeliverySchedules().get(0).getRequestQuantity();
    	}
    }
    double remainingQty = totalParentRQ - sumOfAllCRS;
    return remainingQty;

  }

  public static Map<Long, Double> getRemainingQtyforAllParentRS(EnhancedOrder parentOrder,PlatformUserContext context) {
	  return getRemainingQtyforAllParentRS(parentOrder,null,context);
  }
  
  public static List<EnhancedOrder> getAllChildOrders(Long sysOrderId,PlatformUserContext context) {
	  SqlParams params = new SqlParams();
	  params.setLongValue("SYS_PARENT_ORDER_ID", sysOrderId);
	  List<EnhancedOrder> childOrders = new ArrayList<>();
	  childOrders = DirectModelAccess.read(
			  EnhancedOrder.class,
			  context,
			  params,
			  ModelQuery.sqlFilter("SYS_PARENT_ORDER_ID in $SYS_PARENT_ORDER_ID$"));
	  return childOrders;
  }
  

  /**
   * Get the remaining quantity 
   *
   * @param parentOrder
   * @param context 
   * @param childOrders 
   * @return 
   */
  public static Map<Long, Double> getRemainingQtyforAllParentRS(
    EnhancedOrder parentOrder,List<EnhancedOrder> childOrders,
    PlatformUserContext context) {
    Map<Long, Double> parentRsRemainingQty = new HashMap<>();
    List<String> deadStates = ListUtil.create(States.CANCELLED, States.VENDOR_REJECTED, States.DELETED);
    if(Objects.isNull(childOrders)) {
    	childOrders = OrderUtil.getExistingChildOrders(parentOrder, null, context);
    } else {
    	childOrders = childOrders.stream().filter(order -> !deadStates.contains(order.getState())).collect(Collectors.toList());
    }
    if (childOrders.size() > 0) {
      
      List<RequestSchedule> childReqSchs = new ArrayList<>();
      List<OrderLine> childLines = new ArrayList<>();
      for (EnhancedOrder childOrder : childOrders) {
        childLines.addAll(childOrder.getOrderLines().stream().filter(orderOl -> 
        		!deadStates.contains(orderOl.getState())).collect(Collectors.toList()));
      }
      for (OrderLine parentLine : parentOrder.getOrderLines()) {
        if (OrderUtil.nonTransitionalStates.contains(parentLine.getState())) {
          continue;
        }
        List<OrderLine> childOrderLines = OrderUtil.getRelaventChildLines(parentLine, childLines);
        List<RequestSchedule> relaventRS = new ArrayList<>();
        childOrderLines.stream().forEach(childOl -> {
          relaventRS.addAll(OrderUtil.getAllRequestSchedules(childOl).stream().filter(orderOlRS ->
          			!deadStates.contains(orderOlRS.getState())).collect(Collectors.toList()));
        });
        for (RequestSchedule parentRS : parentLine.getRequestSchedules()) {
          if (OrderUtil.collaborationStates.contains(parentRS.getState())
            || OrderUtil.nonTransitionalStates.contains(parentLine.getState())) {
            continue;
          }
          childReqSchs = OrderUtil.getRelaventChildRS(parentRS, relaventRS);
          if (childReqSchs.isEmpty()) {
            childReqSchs = OrderUtil.getRelaventRSByMatchingItem(parentRS, relaventRS);
          }
          parentRsRemainingQty.put(parentRS.getSysId(), getRemainingQtyFromParentRS(parentRS, childReqSchs,context));
        }
      }
    }
    return parentRsRemainingQty;
  }

  /**
   * TODO complete method documentation
   *
   * @param orderList
   * @param updateChildOrders
   * @param sourceModel 
   * @param platformUserContext
   */
  public static void createRelaventChildOrders(
    List<EnhancedOrder> orderList,
    boolean updateChildOrders, EnhancedOrder sourceModel,
    PlatformUserContext platformUserContext) {
    // TODO Auto-generated method stub
    ModelList<EnhancedOrder> poModelList = null;
    UserContextService contextService = Services.get(UserContextService.class);
    PlatformUserContext vcAdminCtx = contextService.createDefaultValueChainAdminContext(
      platformUserContext.getValueChainId());
    if(!orderList.isEmpty()) {
      OrderUtil.populateTemplateId(orderList);
    }
    if(orderList.size() > 0 && !updateChildOrders) {
      poModelList =  ModelDataServiceUtil.writeModels(EnhancedOrder.STANDARD_MODEL_NAME, Actions.CREATE_PO, orderList,true, vcAdminCtx); 
    }
    if(null != poModelList && !poModelList.getErrors().isEmpty()) {
      LOG.info("Error while fulfilling the order with order type(purchase order) "+poModelList.getErrors().toString());
      sourceModel.setError("Error while fulfilling the order with order type(purchase order) "+poModelList.getErrors().toString());
    }
    
  }

  /**
   * TODO complete method documentation
   *
   * @param orderList
   * @param updateChildOrders
   * @param sourceModel 
   * @param platformUserContext
   */
  public static void updateRelaventChildDeplOrders(
    List<EnhancedOrder> updateChildOrders,
    boolean updateCO,
    EnhancedOrder sourceModel, PlatformUserContext platformUserContext) {
    // TODO Auto-generated method stub
    UserContextService contextService = Services.get(UserContextService.class);
    PlatformUserContext vcAdminCtx = contextService.createDefaultValueChainAdminContext(
      platformUserContext.getValueChainId());
    ModelList<EnhancedOrder> doAAModelList = null, doNewModelList = null, doPostNEWModelList = null, doSyncModelList = null;

    if (updateCO) {
      if (Objects.nonNull(updateChildOrders)) {
        List<EnhancedOrder> childOrdersAwaiting = updateChildOrders.stream().filter(
          order -> (order.getState().equalsIgnoreCase(States.AWAITING_APPROVAL) || order.getState().equalsIgnoreCase(
            com.ordermgmtsystem.supplychaincore.mpt.SCCEnhancedOrderConstants.States.DRAFT))).collect(Collectors.toList());
        List<EnhancedOrder> childOrdersNEW = updateChildOrders.stream().filter(
          order -> (order.getState().equalsIgnoreCase(States.NEW))).collect(Collectors.toList());
        List<EnhancedOrder> childOrdersinBCR = updateChildOrders.stream().filter(
          order -> (order.getState().equalsIgnoreCase(States.BUYER_CHANGE_REQUESTED))).collect(Collectors.toList());
        if (childOrdersAwaiting.size() > 0)
          doAAModelList =ModelDataServiceUtil.writeModels(
            EnhancedOrder.STANDARD_MODEL_NAME,
            SCCEnhancedOrderConstants.Actions.UPDATE_DO,
            childOrdersAwaiting,
            vcAdminCtx);
        if(null != doAAModelList && !doAAModelList.getErrors().isEmpty()) {
          sourceModel.setError("Error while fulfilling the order with order type(Deployment order) "+ doAAModelList.getErrors().toString());
        }
        if (childOrdersNEW.size() > 0)
          doNewModelList = ModelDataServiceUtil.writeModels(
            EnhancedOrder.STANDARD_MODEL_NAME,
            SCCEnhancedOrderConstants.Actions.REVISE,
            childOrdersNEW,
            vcAdminCtx);
        if(null != doNewModelList && !doNewModelList.getErrors().isEmpty()) {
          sourceModel.setError("Error while fulfilling the order with order type(Deployment order) "+ doNewModelList.getErrors().toString());
        }
        boolean allowCollobaration = getOrderSitePolicy(
          updateChildOrders.get(0),
          Policy.ENABLE_COLLABORATION_FOR_DO,
          platformUserContext);

        if (allowCollobaration) {
          if (childOrdersinBCR.size() > 0)
            doPostNEWModelList = ModelDataServiceUtil.writeModels(
              EnhancedOrder.STANDARD_MODEL_NAME,
              SCCEnhancedOrderConstants.Actions.CONSIGNEE_CHANGE_REQUEST,
              childOrdersinBCR,
              vcAdminCtx);
          if(null != doPostNEWModelList && !doPostNEWModelList.getErrors().isEmpty()) {
            sourceModel.setError("Error while fulfilling the order with order type(Deployment order) "+ doPostNEWModelList.getErrors().toString());
          }
          List<EnhancedOrder> childOrdersForSynchDO = updateChildOrders.stream().filter(
            order -> (order.getState().equalsIgnoreCase(States.OPEN)
              || order.getState().equalsIgnoreCase(States.VENDOR_CHANGE_REQUESTED)
              || order.getState().equalsIgnoreCase(States.VENDOR_CONFIRMED_WITH_CHANGES)
              || order.getState().equalsIgnoreCase(States.BUYER_CONFIRMED_WITH_CHANGES))).collect(Collectors.toList());

          if (childOrdersForSynchDO.size() > 0)
            for (EnhancedOrder childOrder : childOrdersForSynchDO) {
              for (OrderLine ol : childOrder.getOrderLines()) {
                for (RequestSchedule rs : ol.getRequestSchedules()) {
                  //                if (Objects.nonNull(rs.getTransientField("revisitState")) && rs.getTransientField("revisitState").equals(true)) {
                  if (!Objects.nonNull(rs.getState())) {
                    rs.setState(States.BUYER_CHANGE_REQUESTED);
                  }
                }
                ol.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(ol)));
              }
              childOrder.setState(OrderUtil.getEffectiveState(OrderUtil.getChildrenStateList(childOrder)));
            }

          doSyncModelList = ModelDataServiceUtil.writeModels(
            EnhancedOrder.STANDARD_MODEL_NAME,
            SCCEnhancedOrderConstants.Actions.SYNCH_DO,
            childOrdersForSynchDO,
            vcAdminCtx);
          if(null != doSyncModelList && !doSyncModelList.getErrors().isEmpty()) {
            sourceModel.setError("Error while fulfilling the order with order type(Deployment order) "+ doSyncModelList.getErrors().toString());
          }
        }
      }
    }
  }


  /**
   * TODO complete method documentation
   *
   * @param orderList
   * @param updateChildOrders
   * @param sourceModel 
   * @param platformUserContext
   */
  public static void createRelaventDeplChildOrders(
    List<EnhancedOrder> orderList,
    boolean updateChildOrders,
    EnhancedOrder sourceModel, PlatformUserContext platformUserContext) {
    // TODO Auto-generated method stub
    ModelList<EnhancedOrder> doModelList = null;
    UserContextService contextService = Services.get(UserContextService.class);
    PlatformUserContext vcAdminCtx = contextService.createDefaultValueChainAdminContext(
      platformUserContext.getValueChainId());
    if (!orderList.isEmpty()) {
      OrderUtil.populateTemplateId(orderList);
    }
    if (orderList.size() > 0 && !updateChildOrders) {
      doModelList = ModelDataServiceUtil.writeModels(
        EnhancedOrder.STANDARD_MODEL_NAME,
        Actions.CREATE_DO,
        orderList,
        true,
        vcAdminCtx);
    }
    if (null != doModelList && !doModelList.getErrors().isEmpty()) {
      LOG.info(
        "Error while fulfilling the order with order type(Deployment order) " + doModelList.getErrors().toString());
    }
  }

  /**
   * Validate HTS code
   *
   * @param htsCode
   * @return
   */
  public static boolean validateHTSCode(String htsCode) {
    htsCode = htsCode.replaceAll("\\.", "");
    Matcher m = HTS_CODE_PATTERN.matcher(htsCode);
    return m.matches();
  }
}
