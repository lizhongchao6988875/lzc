package com.yonyoucloud.fi.epcm.open.service.utils;


import com.yonyou.iuap.BusinessException;
import com.yonyou.iuap.ucf.common.i18n.InternationalUtils;
import com.yonyoucloud.fi.basecom.pubarch.adapter.utils.FiContextUtil;
import com.yonyoucloud.fi.cost.common.util.PartitionList;
import com.yonyoucloud.fi.epcm.accounting.api.IAccountInfoService;
import com.yonyoucloud.fi.epcm.accounting.vo.OnpChangeVO;
import com.yonyoucloud.fi.epcm.common.api.IEpcmLogAppService;
import com.yonyoucloud.fi.epcm.common.repository.IAccInfoRepository;
import com.yonyoucloud.fi.epcm.common.vo.AccInfoVO;
import com.yonyoucloud.fi.epcm.consts.FieldConsts;
import com.yonyoucloud.fi.epcm.costcenter.adapter.ICostCenterVoucherAdapter;
import com.yonyoucloud.fi.epcm.datasheet.ProduceTotalQua;
import com.yonyoucloud.fi.epcm.datasheet.bo.realwktime.RealWkTimeBO;
import com.yonyoucloud.fi.epcm.datasheet.repository.IOperationProduceQuaRepository;
import com.yonyoucloud.fi.epcm.datasheet.repository.IRealWkTimeRepository;
import com.yonyoucloud.fi.epcm.epub.adapter.IAccBookAdapter;
import com.yonyoucloud.fi.epcm.exception.errorcode.EPCMBaseDocErrorCode;
import com.yonyoucloud.fi.epcm.material.adapter.IEpcmMaterialAdapter;
import com.yonyoucloud.fi.epcm.open.api.IProcductCostService;
import com.yonyoucloud.fi.epcm.open.dto.OrderBusinessDateDTO;
import com.yonyoucloud.fi.epcm.open.dto.ProcessAccountDTO;
import com.yonyoucloud.fi.epcm.open.vo.*;
import com.yonyoucloud.fi.epcm.order.bo.enums.EnumBusDateBillAction;
import com.yonyoucloud.fi.epcm.order.bo.enums.EnumBusDateBillType;
import com.yonyoucloud.fi.epcm.realco.RealCo;
import com.yonyoucloud.fi.epcm.realco.RealCoOrderMap;
import com.yonyoucloud.fi.epcm.realco.adapter.IRealCoAdapter;
import com.yonyoucloud.fi.epcm.realco.api.IRealCoOrderAppService;
import com.yonyoucloud.fi.epcm.realco.bo.PeriodRealCoBO;
import com.yonyoucloud.fi.epcm.realco.bo.RealCoOrderMapBO;
import com.yonyoucloud.fi.epcm.realco.repository.IPeriodRealCoRepository;
import com.yonyoucloud.fi.epcm.realco.repository.IRealCoOrderMapRepository;
import com.yonyoucloud.fi.epcm.realco.repository.IRealCoRepository;
import com.yonyoucloud.fi.epcm.realco.vo.CheckLedgerDataVO;
import com.yonyoucloud.fi.epcm.realco.vo.MaterialOrderAccountVO;
import com.yonyoucloud.fi.epcm.rule.bo.AccountingDimDetailBO;
import com.yonyoucloud.fi.epcm.rule.repository.ICostParametersRepository;
import com.yonyoucloud.fi.epcm.rule.vo.CostParametersVO;
import com.yonyoucloud.fi.epcm.utils.BizUtils;
import com.yonyoucloud.fi.ma.types.enumtype.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.imeta.core.lang.BooleanUtils;
import org.imeta.spring.support.id.IdManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @program: fipcm
 * @description
 * @author: LZC
 * @create: 2021-11-08 16:31
 **/
@Service
public class ProductCostServiceImpl implements IProcductCostService {
    private static Logger logger = LoggerFactory.getLogger(ProductCostServiceImpl.class);
    private static ThreadLocal<DateFormat> sdfYM = new ThreadLocal<DateFormat>() {
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM");
        }
    };
    private static ThreadLocal<DateFormat> sdfYMD = new ThreadLocal<DateFormat>() {
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };

    @Autowired
    @Qualifier("pcm.accountInfoService")
    private IAccountInfoService accountinfoService;
    @Autowired
    private IRealCoOrderAppService realCoOrderAppService;
    @Autowired
    private ICostParametersRepository iCostParametersRepository;
    @Autowired
    private IRealCoOrderMapRepository realCoOrderMapRepository;
    @Autowired
    private IPeriodRealCoRepository periodRealCoRepository;
    @Autowired
    private IAccInfoRepository accInfoRepository;
    @Autowired
    private IRealCoRepository realCoRepository;
    @Autowired
    private IEpcmMaterialAdapter materialService;
    @Autowired
    private IRealCoAdapter realCoAdapter;
    @Autowired
    private IOperationProduceQuaRepository operationProduceQuaRepository;
    @Autowired
    private IRealWkTimeRepository realWkTimeRepository;
    @Autowired
    private ICostCenterVoucherAdapter costCenterVoucherAdapter;
    @Autowired
    private IEpcmLogAppService epcmLogService;
    @Autowired
    private IAccBookAdapter accBookAdapter;

    @Override
    public String getAccountPeriodByAccentity(String accentity) {
        try {
            return accountinfoService.getAccountPeriodByAccentity(accentity);
        } catch (Exception ex) {
            throw new BusinessException("032-502-200072", com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F80065", "判断指定会计主体和账簿在某一期间是否结账出现异常！") /* "判断指定会计主体和账簿在某一期间是否结账出现异常！" */, ex);
        }
    }

    @Override
    public AccountInfoVO getAccountDateByAccentity(String accentity) {
        try {
            return accountinfoService.getAccountDateByAccentity(accentity);
        } catch (Exception ex) {
            throw new BusinessException("032-502-200073", com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F8005D", "根据会计主体查询成本关账日期出现异常！") /* "根据会计主体查询成本关账日期出现异常！" */, ex);
        }
    }

    @Override
    public AccountInfoVO getAccountDateByFactoryId(String factoryId) {
        try {
            return accountinfoService.getAccountDateByFactory(factoryId);
        } catch (Exception ex) {
            throw new BusinessException("032-502-200074", com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F80062", "根据工厂查询成本关账日期出现异常！") /* "根据工厂查询成本关账日期出现异常！" */, ex);
        }
    }

    @Override
    public List<RealCoOrderVO> orderIsMeetOpen(List<Long> orderRowIds) {
        try {
            return realCoOrderAppService.orderIsMeetOpen(orderRowIds);
        } catch (Exception ex) {
            throw new BusinessException("032-502-200075", com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F80065", "判断指定会计主体和账簿在某一期间是否结账出现异常！") /* "判断指定会计主体和账簿在某一期间是否结账出现异常！" */, ex);
        }
    }

    @Override
    public List<OrderPeriodVO> getCloseDatePeriodEndDate(List<OrderPeriodVO> params) {
        try {
            return accountinfoService.getCloseDatePeriodEndDate(params);
        } catch (Exception ex) {
            throw new BusinessException("032-502-200076", com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F8005C", "判断订单是否满足弃审出现异常！") /* "判断订单是否满足弃审出现异常！" */, ex);
        }
    }


    @Override
    public ResponseResult batchFinancialClose(List<String> orderRowIds) {
        ResponseResult result = new ResponseResult();
        result.setCode(ResponseCode.Success);
        if (CollectionUtils.isEmpty(orderRowIds)) {
            return result;
        }
        List<PeriodRealCoBO> updatePeriodRealCos = new ArrayList<>(orderRowIds.size());
        List<String> meetOrderRowIds = new ArrayList<>(orderRowIds.size());
        List<String> noMeetOrderRowIds = new ArrayList<>(orderRowIds.size());
        List<String> meetRealCoIds = new ArrayList<>(orderRowIds.size());
        List<String> messages = new ArrayList<>(orderRowIds.size());
        try {
            // 批量查询核算关联订单
            List<RealCoOrderMapBO> realCoOrderMapBos = new ArrayList<>(orderRowIds.size());
            List<List<String>> batchRequestOrderMap = PartitionList.partition(orderRowIds, 500);
            for (List<String> requestDatas : batchRequestOrderMap) {
                String[] fields = new String[]{RealCoOrderMap.REALCO, RealCoOrderMap.ORDERSTATUS, RealCoOrderMap.REALCO + "." + RealCo.OBJTYPE + " as " + RealCo.OBJTYPE, RealCoOrderMap.ORDERROWID, "realCo.mainCo.accounts.accbook as accbook",
                        "realCo.mainCo.accounts.accentity as accentity"};
                List<RealCoOrderMapBO> queryDatas = realCoOrderMapRepository.queryRealCoOrderMapByOrderRowIds(requestDatas, fields);
                if (!CollectionUtils.isEmpty(queryDatas)) {
                    realCoOrderMapBos.addAll(queryDatas);
                }
            }
            if (CollectionUtils.isEmpty(realCoOrderMapBos)) {
                return result;
            }

            Set<String> accentitys = new HashSet<>(realCoOrderMapBos.size());
            Set<String> accbooks = new HashSet<>(realCoOrderMapBos.size());
            Map<String, AccInfoVO> accInfoGroupMaps = new HashMap<>();
            realCoOrderMapBos.forEach(item -> {
                accentitys.add(item.getAccentity());
                accbooks.add(item.getAccbook());
            });

            if (!CollectionUtils.isEmpty(accentitys)) {
                // 批量查询账簿核算期间
                List<AccInfoVO> accountInfos = accInfoRepository.batchQueryCurrentPeriod(new ArrayList<>(accentitys), new ArrayList<>(accbooks));
                if (!CollectionUtils.isEmpty(accountInfos)) {
                    accInfoGroupMaps = accountInfos.stream().collect(Collectors.toMap(item -> item.getAccentity() + "_" + item.getAccbook(), item -> item, (x, y) -> y));
                }
            }

            List<String> realCoIds = realCoOrderMapBos.stream().map(RealCoOrderMapBO::getRealCo).distinct().collect(Collectors.toList());
            List<PeriodRealCoBO> periodBos = periodRealCoRepository.queryPeriodByRealCoIds(realCoIds);
            Map<String, PeriodRealCoBO> periodRealCoMap = periodBos.stream().collect(Collectors.toMap(item -> item.getRealCo() + "_" + item.getAccentity() + "_" + item.getAccbook(), item -> item, (x, y) -> y));

            Map<String, List<RealCoOrderMapBO>> groupOrderMap = realCoOrderMapBos.stream().collect(Collectors.groupingBy(item -> item.getOrderRowId().toString()));
            List<String> singleRealCoIds = new ArrayList<>();
            for (String orderRowId : orderRowIds) {
                List<RealCoOrderMapBO> datas = groupOrderMap.get(orderRowId);
                for (RealCoOrderMapBO orderMapBo : datas) {
                    if (!EnumOrderStatus.OS_CLOSE.getValue().equals(orderMapBo.getOrderStatus())) {
                        messages.add(orderMapBo.getOrderRowId() + com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F80067", "该订单未关闭,不能财务关闭!") /* "该订单未关闭,不能财务关闭!" */);
                        noMeetOrderRowIds.add(orderMapBo.getOrderRowId().toString());
                        continue;
                    }
                    if (EnumObjType.Account_Product.getValue().equals(orderMapBo.getObjType())) {
                        //meetOrderRowIds.add(orderMapBo.getOrderRowId().toString());
                        singleRealCoIds.add(orderMapBo.getRealCo());
                        continue;
                    }
                    String realCoId = orderMapBo.getRealCo();
                    String accentity = orderMapBo.getAccentity();
                    String accbook = orderMapBo.getAccbook();
                    AccInfoVO accInfoVO = accInfoGroupMaps.get(accentity + "_" + accbook);
                    if (Objects.isNull(accInfoVO)) {
                        continue;
                    }
                    String key = realCoId + "_" + accentity + "_" + accbook;
                    PeriodRealCoBO periodRealCo = periodRealCoMap.get(key);
                    if (Objects.isNull(periodRealCo)) {
                        continue;
                    }
                    periodRealCo.setEndPeriodId(accInfoVO.getPeriod());
                    periodRealCo.setEndPeriodCode(accInfoVO.getPeriodCode());
                    updatePeriodRealCos.add(periodRealCo);
                    meetRealCoIds.add(orderMapBo.getRealCo());
                    meetOrderRowIds.add(orderMapBo.getOrderRowId().toString());
                }
            }
            if (CollectionUtils.isEmpty(singleRealCoIds)) {
                periodRealCoRepository.updateAccClosedStatus(updatePeriodRealCos, meetOrderRowIds, EnumAccClosedStatus.AS_SUCCESS.getValue());
                result.setData(noMeetOrderRowIds);
                result.setErrorSource(messages);
                return result;
            }

            // 根据单品realCoIds查询所有订单判断是否满足财务关闭
            List<RealCoOrderMapBO> singleRealCoOrderMapBos = new ArrayList<>();
            List<List<String>> batchRequestFinStatus = PartitionList.partition(singleRealCoIds, 500);
            for (List<String> requestDatas : batchRequestFinStatus) {
                List<RealCoOrderMapBO> datas = realCoOrderMapRepository.queryUnionOrderProduct(requestDatas);
                if (!CollectionUtils.isEmpty(datas)) {
                    singleRealCoOrderMapBos.addAll(datas);
                }
            }
            Map<String, List<RealCoOrderMapBO>> singleRealCoOrderMap = singleRealCoOrderMapBos.stream().collect(Collectors.groupingBy(RealCoOrderMapBO::getRealCo));
            Map<String, String> orderRowIdMap = orderRowIds.stream().collect(Collectors.toMap(i -> i, m -> m));

            for (Map.Entry<String, List<RealCoOrderMapBO>> entry : singleRealCoOrderMap.entrySet()) {
                String realCoId = entry.getKey();
                List<RealCoOrderMapBO> groupList = entry.getValue();
                if (CollectionUtils.isEmpty(groupList)) {
                    continue;
                }
                boolean ifAccClosed = true;
                for (RealCoOrderMapBO item : groupList) {
                    if (EnumAccClosedStatus.AS_NOCLOSE.getValue().equals(item.getAccClosed()) && Objects.isNull(orderRowIdMap.get(item.getOrderRowId().toString()))) {
                        ifAccClosed = false;
                        break;
                    }
                }
                if (ifAccClosed) {
                    String accentity = groupList.get(0).getAccentity();
                    String accbook = groupList.get(0).getAccbook();
                    AccInfoVO accInfoVO = accInfoGroupMaps.get(accentity + "_" + accbook);
                    if (Objects.isNull(accInfoVO)) {
                        continue;
                    }
                    String key = realCoId + "_" + accentity + "_" + accbook;
                    PeriodRealCoBO periodRealCo = periodRealCoMap.get(key);
                    if (Objects.isNull(periodRealCo)) {
                        continue;
                    }
                    periodRealCo.setEndPeriodId(accInfoVO.getPeriod());
                    periodRealCo.setEndPeriodCode(accInfoVO.getPeriodCode());
                    updatePeriodRealCos.add(periodRealCo);
                    meetRealCoIds.add(realCoId);
                    continue;
                }
                noMeetOrderRowIds.add(groupList.get(0).getOrderRowId().toString());
                messages.add(groupList.get(0).getOrderRowId() + com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F8005E", "该单品订单下有订单未关闭,不能财务关闭!") /* "该单品订单下有订单未关闭,不能财务关闭!" */);
            }
            periodRealCoRepository.updateAccClosedStatus(updatePeriodRealCos, meetOrderRowIds, EnumAccClosedStatus.AS_SUCCESS.getValue());
        } catch (Exception ex) {
            result.setCode(ResponseCode.Failure);
            result.setMsg(com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F80063", "订单财务关闭失败,错误信息:") /* "订单财务关闭失败,错误信息:" */ + ex.getMessage());
            logger.error("================ batchFinancialClose 订单财务关闭失败,错误信息 ===================", ex);
        }
        result.setData(noMeetOrderRowIds);
        result.setErrorSource(messages);
        return result;
    }

    @Override
    public ResponseResult batchFinancialOpen(List<String> orderRowIds) {
        ResponseResult result = new ResponseResult();
        result.setCode(ResponseCode.Success);
        if (CollectionUtils.isEmpty(orderRowIds)) {
            return result;
        }
        List<PeriodRealCoBO> updatePeriodRealCos = new ArrayList<>(orderRowIds.size());
        try {
            // 去重
            List<String> distinctOrderRowIds = orderRowIds.stream().distinct().collect(Collectors.toList());
            result.setData(distinctOrderRowIds);
            // 批量查询核算关联订单
            List<RealCoOrderMapBO> realCoOrderMapBos = new ArrayList<>(distinctOrderRowIds.size());
            List<List<String>> batchRequest = PartitionList.partition(distinctOrderRowIds, 500);
            for (List<String> requestDatas : batchRequest) {
                String[] fields = new String[]{RealCoOrderMap.REALCO, RealCoOrderMap.ORDERROWID, "realCo.mainCo.accounts.accbook as accbook",
                        "realCo.mainCo.accounts.accentity as accentity"};
                List<RealCoOrderMapBO> queryDatas = realCoOrderMapRepository.queryRealCoOrderMapByOrderRowIds(requestDatas, fields);
                if (!CollectionUtils.isEmpty(queryDatas)) {
                    realCoOrderMapBos.addAll(queryDatas);
                }
            }
            if (CollectionUtils.isEmpty(realCoOrderMapBos)) {
                return result;
            }
            Set<String> accentitys = new HashSet<>(realCoOrderMapBos.size());
            Set<String> accbooks = new HashSet<>(realCoOrderMapBos.size());
            realCoOrderMapBos.forEach(item -> {
                accentitys.add(item.getAccentity());
                accbooks.add(item.getAccbook());
            });

            List<String> realCoIds = realCoOrderMapBos.stream().map(RealCoOrderMapBO::getRealCo).distinct().collect(Collectors.toList());
            List<PeriodRealCoBO> periodBos = periodRealCoRepository.queryPeriodByRealCoIds(realCoIds);
            Map<String, PeriodRealCoBO> periodRealCoMap = periodBos.stream().collect(Collectors.toMap(item -> item.getRealCo() + "_" + item.getAccentity() + "_" + item.getAccbook(), item -> item, (x, y) -> y));

            for (RealCoOrderMapBO realCoOrderMapBo : realCoOrderMapBos) {
                String realCoId = realCoOrderMapBo.getRealCo();
                String accentity = realCoOrderMapBo.getAccentity();
                String accbook = realCoOrderMapBo.getAccbook();
                String key = realCoId + "_" + accentity + "_" + accbook;
                PeriodRealCoBO periodRealCo = periodRealCoMap.get(key);
                if (Objects.isNull(periodRealCo)) {
                    continue;
                }
                periodRealCo.setEndPeriodId(null);
                periodRealCo.setEndPeriodCode(null);
                updatePeriodRealCos.add(periodRealCo);
            }
            // 更新财务状态
            periodRealCoRepository.updateAccClosedStatus(updatePeriodRealCos, distinctOrderRowIds, EnumAccClosedStatus.AS_NOCLOSE.getValue());
        } catch (Exception ex) {
            result.setCode(ResponseCode.Failure);
            result.setMsg(com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F80061", "订单财务打开失败,错误信息:") /* "订单财务打开失败,错误信息:" */ + ex.getMessage());
            logger.error("================ batchFinancialOpen 订单财务打开失败,错误信息 ===================", ex);
        }

        return result;
    }

    @Override
    public List<RealCoOrderVO> checkIfMeetChangeCharacter(List<Long> orderRowIds) {
        List<RealCoOrderVO> result = new ArrayList<>();
        if (CollectionUtils.isEmpty(orderRowIds)) {
            return result;
        }
        try {
            // 批量查询核算关联订单
            List<RealCoOrderMapBO> realCoOrderMapBos = new ArrayList<>(orderRowIds.size());
            List<List<Long>> batchRequest = PartitionList.partition(orderRowIds, 500);
            for (List<Long> requestDatas : batchRequest) {
                String[] fields = new String[]{RealCoOrderMap.REALCO, RealCoOrderMap.ORDERCODE, RealCoOrderMap.ORDERROWID, RealCoOrderMap.REALCO + "." + RealCo.OBJTYPE + " as objType"};
                List<RealCoOrderMapBO> queryDatas = realCoOrderMapRepository.queryRealCoOrderMapByOrderRowIds(requestDatas, fields);
                if (!CollectionUtils.isEmpty(queryDatas)) {
                    realCoOrderMapBos.addAll(queryDatas);
                }
            }
            if (CollectionUtils.isEmpty(realCoOrderMapBos)) {
                result = pageResultData(orderRowIds, EPCMBaseDocErrorCode.ERROR_137);
                return result;
            }
            Map<Long, List<RealCoOrderMapBO>> orderMaps = realCoOrderMapBos.stream().collect(Collectors.groupingBy(RealCoOrderMapBO::getOrderRowId));
            List<String> realCoIds = realCoOrderMapBos.stream().map(RealCoOrderMapBO::getRealCo).distinct().collect(Collectors.toList());
            // 查询核算对象是否已做期初期末调整
            List<OnpChangeVO> onpChanges = realCoRepository.queryQcOnpChange(realCoIds);
            Map<String, List<OnpChangeVO>> onpChangeMap = onpChanges.stream().collect(Collectors.groupingBy(OnpChangeVO::getRealCoId));

            for (Long orderRowId : orderRowIds) {
                RealCoOrderVO vo = new RealCoOrderVO();
                vo.setOrderRowId(orderRowId);
                vo.setMeetChange(true);
                List<RealCoOrderMapBO> values = orderMaps.get(orderRowId);
                if (CollectionUtils.isEmpty(values)) {
                    vo.setMeetChange(false);
                    vo.setMessage(values.get(0).getOrderCode() + InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1BB3B7E405B80008", "订单在产品成本下产品成本对象中未查询到,不能变更特征值！"));
                    result.add(vo);
                    continue;
                }
                boolean ifExistSingle = values.stream().anyMatch(item -> (EnumObjType.Account_Product.getValue().equals(item.getObjType())));
                if (ifExistSingle) {
                    vo.setMeetChange(false);
                    vo.setMessage(values.get(0).getOrderCode() + InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1B74A88605480003", "该订单已在产品成本对象中生成单品订单,不能变更特征值！"));
                    result.add(vo);
                    continue;
                }
                boolean ifExistQcOnpChange = values.stream().anyMatch(item -> !CollectionUtils.isEmpty(onpChangeMap.get(item.getRealCo())));
                if (ifExistQcOnpChange) {
                    vo.setMeetChange(false);
                    vo.setMessage(values.get(0).getOrderCode() + InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1BB3B86C05B80007", "订单已在产品成本生成期初或期末调整,不能变更特征值！"));
                }
                result.add(vo);
            }
            return result;
        } catch (Exception ex) {
            throw new BusinessException("032-502-200078", com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F8005F", "校验订单是否可以变更特征值出现异常！") /* "校验订单是否可以变更特征值出现异常！" */, ex);
        }
    }

    @Override
    public List<ProcessAccountDTO> orderIsProcessAccount(List<ProcessAccountDTO> processAccountDtos) {
        if (CollectionUtils.isEmpty(processAccountDtos)) {
            return new ArrayList<>();
        }
        try {
            List<String> accentitys = processAccountDtos.stream().map(ProcessAccountDTO::getAccentity).collect(Collectors.toList());
            // 查询产品成本参数
            List<CostParametersVO> costParametersVos = iCostParametersRepository.queryCostParameterByAccentity(accentitys);
            Map<String, List<CostParametersVO>> costParamerMap = costParametersVos.stream().filter(i -> EnumAccountingMethodSetting.Account_AccentityCustom.getValue().equals(i.getAccountingMethodSetting())).collect(Collectors.groupingBy(CostParametersVO::getAccentity));
            Map<String, List<CostParametersVO>> grpAccentityFactoryMap = costParametersVos.stream().filter(i -> EnumAccountingMethodSetting.Account_FactoryCustom.getValue().equals(i.getAccountingMethodSetting())).collect(Collectors.groupingBy(i -> i.getAccentity() + "_" + i.getFactory()));

            // 查询成本分类（为了查询核算物料方法组装数据）
            List<Long> productIds = processAccountDtos.stream().map(ProcessAccountDTO::getProductId).collect(Collectors.toList());
            Map<Long, Object> productClassMap = materialService.queryMaterialClass(new ArrayList<>(productIds));

            // 查询核算物料是否按生产订单核算
            List<MaterialOrderAccountVO> params = new ArrayList<>();
            processAccountDtos.forEach(i -> {
                List<CostParametersVO> parametersVos = grpAccentityFactoryMap.get(i.getAccentity() + "_" + i.getFactory());
                if (CollectionUtils.isEmpty(parametersVos)) {
                    return;
                }
                List<Long> materialIds = new ArrayList<>();
                materialIds.add(i.getProductId());
                parametersVos.forEach(n -> {
                    MaterialOrderAccountVO vo = new MaterialOrderAccountVO();
                    vo.setAccentity(n.getAccentity());
                    vo.setAccbook(n.getAccbook());
                    vo.setFactory(i.getFactory());
                    vo.setAccpurpose(n.getAccpurpose());
                    vo.setMaterialIds(materialIds);
                    params.add(vo);
                });
            });
            Map<String, Boolean> productIsOrderAccountMap = realCoAdapter.queryMaterialIsOrderAccount(params, productClassMap);

            for (ProcessAccountDTO processAccountDto : processAccountDtos) {
                processAccountDto.setIfProcessAccount(false);
                List<CostParametersVO> parametersVos = grpAccentityFactoryMap.get(processAccountDto.getAccentity() + "_" + processAccountDto.getFactory());
                if (CollectionUtils.isEmpty(parametersVos)) {
                    parametersVos = costParamerMap.get(processAccountDto.getAccentity());
                }
                if (CollectionUtils.isEmpty(parametersVos)) {
                    // 产品参数为空返回false 允许工序为空
                    continue;
                }
                if (!parametersVos.get(0).getAccountProcess()) {
                    // 非工序 允许工序为空
                    continue;
                }
                Boolean isOrderAccount = productIsOrderAccountMap.get(processAccountDto.getFactory() + "_" + processAccountDto.getProductId());
                if (Objects.nonNull(isOrderAccount) && isOrderAccount) {
                    // 物料按生产订单核算则 工序可以为空
                    continue;
                }
                processAccountDto.setIfProcessAccount(true);
                processAccountDto.setMessage(InternationalUtils.getMessageWithDefault(EPCMBaseDocErrorCode.ERROR_146.getI18nCode(), EPCMBaseDocErrorCode.ERROR_146.getMessageFormat()));
            }

            return processAccountDtos;
        } catch (Exception ex) {
            throw new BusinessException("032-502-200079", com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F80060", "查询订单是否按工序核算出现异常！") /* "查询订单是否按工序核算出现异常！" */, ex);
        }
    }


    @Override
    public List<ProcessAccountDTO> orderIsHappenProcessCost(List<ProcessAccountDTO> processAccountDtos) {
        if (CollectionUtils.isEmpty(processAccountDtos)) {
            return new ArrayList<>();
        }
        try {
            List<Long> orderIds = processAccountDtos.stream().map(ProcessAccountDTO::getOrderId).collect(Collectors.toList());
            List<RealCoOrderMapBO> orderMapBos = realCoRepository.queryValidOrderMapByOrderIds(orderIds);
            if (CollectionUtils.isEmpty(orderMapBos)) {
                processAccountDtos.forEach(i -> i.setCheckIfUpdateOpsn(false));
                return processAccountDtos;
            }
            Map<Long, List<RealCoOrderMapBO>> orderMapByOrderIdMap = orderMapBos.stream().collect(Collectors.groupingBy(RealCoOrderMapBO::getOrderId));
            Map<String, RealCoOrderMapBO> orderRowIdOpSnMap = orderMapBos.stream().collect(Collectors.toMap(item -> item.getOrderRowId() + "_" + item.getOpSn(), item -> item, (key1, key2) -> key2));
            Set<String> realCoIds = new HashSet<>(orderMapBos.size());
            for (ProcessAccountDTO dto : processAccountDtos) {
                List<RealCoOrderMapBO> bos = orderMapByOrderIdMap.get(dto.getOrderId());
                if (CollectionUtils.isEmpty(bos)) {
                    dto.setCheckIfUpdateOpsn(false);
                    continue;
                }
                if (Objects.isNull(dto.getSn())) {
                    dto.setMessage(InternationalUtils.getMessageWithDefault(EPCMBaseDocErrorCode.ERROR_163.getI18nCode(), EPCMBaseDocErrorCode.ERROR_163.getMessageFormat()));
                    dto.setCheckIfUpdateOpsn(true);
                    continue;
                }
                RealCoOrderMapBO bo = orderRowIdOpSnMap.get(dto.getOrderRowId() + "_" + dto.getSn());
                if (Objects.isNull(bo)) {
                    dto.setCheckIfUpdateOpsn(false);
                    continue;
                }
                realCoIds.add(bo.getRealCo());
            }
            //List<String> realCoIds = orderMapBos.stream().map(RealCoOrderMapBO::getRealCo).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(realCoIds)) {
                processAccountDtos.forEach(i -> {
                    if (Objects.isNull(i.getMessage())) {
                        i.setCheckIfUpdateOpsn(false);
                    }
                });
                //processAccountDtos.forEach(i -> i.setCheckIfUpdateOpsn(false));
                return processAccountDtos;
            }
            orderMapBos = orderMapBos.stream().filter(item -> realCoIds.contains(item.getRealCo())).collect(Collectors.toList());
            // 查询工序产量数据
            List<ProduceTotalQua> produceTotalQuaList = operationProduceQuaRepository.queryOperationProduceQuaList(new ArrayList<>(realCoIds));
            // 查询工时数据
            List<RealWkTimeBO> realWkTimeBos = realWkTimeRepository.queryRealWkTimeList(new ArrayList<>(realCoIds));
            // 查询明细账数据
            List<CheckLedgerDataVO> ledgerDataVos = queryLedgerData(processAccountDtos, orderMapBos);

            // 汇总统计是否发生数据
            Map<String, Map<Boolean, String>> orderIsHappenMap = checkOrderIsHappen2(orderMapBos, produceTotalQuaList, realWkTimeBos, ledgerDataVos);
            //Map<Long, Map<Boolean, String>> orderIsHappenMap = checkOrderIsHappen(orderMapBos, produceTotalQuaList, realWkTimeBos, ledgerDataVos);

            for (ProcessAccountDTO processAccountDto : processAccountDtos) {
                if (Objects.nonNull(processAccountDto.getMessage())) {
                    continue;
                }
                processAccountDto.setCheckIfUpdateOpsn(false);
                Map<Boolean, String> dataMap = orderIsHappenMap.get(processAccountDto.getOrderRowId() + "_" + processAccountDto.getSn());
                //Map<Boolean, String> dataMap = orderIsHappenMap.get(processAccountDto.getOrderId());
                if (MapUtils.isEmpty(dataMap)) {
                    continue;
                }
                Map.Entry<Boolean, String> firstEntry = dataMap.entrySet().iterator().next();
                Boolean ifHappenOpCost = firstEntry.getKey();
                String firstValue = firstEntry.getValue();
                if (!ifHappenOpCost) {
                    continue;
                }
                processAccountDto.setCheckIfUpdateOpsn(true);
                processAccountDto.setMessage(firstValue);
            }

            return processAccountDtos;
        } catch (Exception ex) {
            throw new BusinessException("032-502-200080", com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F80068", "判断订单是否发生工序成本出现异常！") /* "判断订单是否发生工序成本出现异常！" */, ex);
        }
    }

    @Override
    public List<RealCoOrderVO> checkIfMeetChangeCostCenter(List<Long> orderRowIds) throws BusinessException {
        try {
            List<RealCoOrderVO> result = new ArrayList<>();
            if (CollectionUtils.isEmpty(orderRowIds)) {
                return result;
            }
            // 批量查询核算关联订单
            List<RealCoOrderMapBO> realCoOrderMapBos = new ArrayList<>(orderRowIds.size());
            List<List<Long>> batchRequest = PartitionList.partition(orderRowIds, 500);
            for (List<Long> requestDatas : batchRequest) {
                String[] fields = new String[]{RealCoOrderMap.REALCO, RealCoOrderMap.ORDERCODE, RealCoOrderMap.ORDERROWID, RealCoOrderMap.PRODUCTTYPE, RealCoOrderMap.REALCO + "." + RealCo.OBJTYPE + " as " + RealCo.OBJTYPE, RealCoOrderMap.FINGER, RealCoOrderMap.ACCOUNTDIMS};
                List<RealCoOrderMapBO> queryDatas = realCoOrderMapRepository.queryRealCoOrderMapByOrderRowIds(requestDatas, fields);
                if (!CollectionUtils.isEmpty(queryDatas)) {
                    realCoOrderMapBos.addAll(queryDatas);
                }
            }
            if (CollectionUtils.isEmpty(realCoOrderMapBos)) {
                result = pageResultData(orderRowIds, EPCMBaseDocErrorCode.ERROR_138);
                return result;
            }
            Map<Long, List<RealCoOrderMapBO>> orderMaps = realCoOrderMapBos.stream().collect(Collectors.groupingBy(RealCoOrderMapBO::getOrderRowId));
            List<String> realCoIds = realCoOrderMapBos.stream().map(RealCoOrderMapBO::getRealCo).distinct().collect(Collectors.toList());
            // 查询核算对象是否已做期初期末调整
            List<OnpChangeVO> onpChanges = realCoRepository.queryQcOnpChange(realCoIds);
            Map<String, List<OnpChangeVO>> onpChangeMap = onpChanges.stream().collect(Collectors.groupingBy(OnpChangeVO::getRealCoId));

            for (Long orderRowId : orderRowIds) {
                RealCoOrderVO vo = new RealCoOrderVO();
                vo.setOrderRowId(orderRowId);
                vo.setMeetChange(true);
                List<RealCoOrderMapBO> values = orderMaps.get(orderRowId);
                if (CollectionUtils.isEmpty(values)) {
                    result.add(vo);
                    continue;
                }
                List<RealCoOrderMapBO> productOrderMapBos = values.stream().filter(item -> EnumProductType.MainProduct.getValue().equals(item.getProductType()) && EnumObjType.Account_Product.getValue().equals(item.getObjType())).collect(Collectors.toList());
                if (!CollectionUtils.isEmpty(productOrderMapBos)) {
                    RealCoOrderMapBO mainBo = productOrderMapBos.get(0);
                    boolean ifContainCostCenter = BizUtils.accountFingerIsContainCostCenter(mainBo.getAccountDims());
                    if (ifContainCostCenter) {
                        vo.setMeetChange(false);
                        vo.setMessage(InternationalUtils.getMessageWithDefault(EPCMBaseDocErrorCode.ERROR_138.getI18nCode(), EPCMBaseDocErrorCode.ERROR_138.getMessageFormat()));
                        result.add(vo);
                        continue;
                    }
                }
                boolean ifExistQcOnpChange = values.stream().anyMatch(item -> !CollectionUtils.isEmpty(onpChangeMap.get(item.getRealCo())));
                if (ifExistQcOnpChange) {
                    vo.setMeetChange(false);
                    vo.setMessage(values.get(0).getOrderCode() + InternationalUtils.getMessageWithDefault(EPCMBaseDocErrorCode.ERROR_145.getI18nCode(), EPCMBaseDocErrorCode.ERROR_145.getMessageFormat()));
                }
                result.add(vo);
            }
            return result;
        } catch (Exception ex) {
            throw new BusinessException("032-502-200210", com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F80066", "检查是否允许修改成本中心失败！") /* "检查是否允许修改成本中心失败！" */, ex);
        }
    }

    @Override
    public List<ProcessAccountDTO> checkCostCenterIsProductAccounting(List<ProcessAccountDTO> processAccountDtos) throws BusinessException {
        try {
            if (CollectionUtils.isEmpty(processAccountDtos)) {
                return new ArrayList<>();
            }

            List<String> accentitys = processAccountDtos.stream().map(ProcessAccountDTO::getAccentity).collect(Collectors.toList());
            // 查询核算账簿(为了查询是否启用停用，停用的账簿不在计算里)
            List<AccInfoVO> accInfoVos = accInfoRepository.batchQueryCurrentPeriods(accentitys);
            if (CollectionUtils.isEmpty(accInfoVos)) {
                processAccountDtos.forEach(i -> i.setCostCenterIsProductAccounting(false));
                return processAccountDtos;
            }
            Map<String, AccInfoVO> accInfoGroupMaps = accInfoVos.stream().collect(Collectors.toMap(item -> item.getAccentity() + "_" + item.getAccbook(), item -> item, (x, y) -> y));
            // 查询产品成本参数
            List<CostParametersVO> costParametersVos = iCostParametersRepository.queryCostParameterDimension(accentitys);
            Map<String, List<CostParametersVO>> grpAccentityMap = costParametersVos.stream().collect(Collectors.groupingBy(CostParametersVO::getAccentity));
            Map<String, List<CostParametersVO>> grpAccentityFactoryMap = costParametersVos.stream().collect(Collectors.groupingBy(item -> item.getAccentity() + "_" + item.getFactory()));
            for (ProcessAccountDTO processAccountDto : processAccountDtos) {
                processAccountDto.setCostCenterIsProductAccounting(false);
                List<CostParametersVO> parametersVos = grpAccentityFactoryMap.get(processAccountDto.getAccentity() + "_" + processAccountDto.getFactory());
                if (CollectionUtils.isEmpty(parametersVos)) {
                    parametersVos = grpAccentityMap.get(processAccountDto.getAccentity());
                }
                if (CollectionUtils.isEmpty(parametersVos)) {
                    continue;
                }
                // 过滤掉停用的账簿
                List<CostParametersVO> costParameters = parametersVos.stream().filter(item -> Objects.nonNull(accInfoGroupMaps.get(item.getAccentity() + "_" + item.getAccbook()))).collect(Collectors.toList());
                costParameters.forEach(item -> {
                    List<AccountingDimDetailBO> accountingDimDetailBos = item.getAccountingDimDetailBos();
                    if (CollectionUtils.isEmpty(accountingDimDetailBos)) {
                        return;
                    }
                    String dimDetails = accountingDimDetailBos.stream().map(AccountingDimDetailBO::getFieldFullName).collect(Collectors.joining(","));
                    try {
                        boolean ifContainCostCenter = BizUtils.accountFingerIsContainCostCenter(dimDetails);
                        if (ifContainCostCenter) {
                            processAccountDto.setCostCenterIsProductAccounting(true);
                            processAccountDto.setMessage(InternationalUtils.getMessageWithDefault(EPCMBaseDocErrorCode.ERROR_150.getI18nCode(), EPCMBaseDocErrorCode.ERROR_150.getMessageFormat()));
                        }
                    } catch (Exception e) {
                    }
                });
            }

            return processAccountDtos;
        } catch (Exception ex) {
            throw new BusinessException("032-502-200211", com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F80064", "检查成本中心是否已经生产核算失败！") /* "检查成本中心是否已经生产核算失败！" */, ex);
        }
    }

    @Override
    public List<OrderBusinessDateDTO> checkBusinessDate(List<OrderBusinessDateDTO> orderBusinessDateDtos) throws BusinessException {
        try {
            // YMS读取“取消结账校验”值，默认为false
            boolean unSettAccountCheck = BooleanUtils.b(FiContextUtil.getEnvConfig("unSetAccountCheckOnCreateCO", "false"));
            if (unSettAccountCheck) {
                for (OrderBusinessDateDTO orderBusinessDateDto : orderBusinessDateDtos) {
                    orderBusinessDateDto.setCode("200");
                }
                return orderBusinessDateDtos;
            }
            for (OrderBusinessDateDTO orderBusinessDateDto : orderBusinessDateDtos) {
                orderBusinessDateDto.setCode("200");
                String billType = orderBusinessDateDto.getBillType();
                String billAction = orderBusinessDateDto.getBillAction();
                String accentity = orderBusinessDateDto.getAccentity();
                Date businessDate = orderBusinessDateDto.getBusinessDate();
                if ((billType.equals(EnumBusDateBillType.POORDER.getValue()) || billType.equals(EnumBusDateBillType.OSMORDER.getValue())) && billAction.equals(EnumBusDateBillAction.AUDIT.getValue())) {
                    AccountInfoVO accountInfoVo = accountinfoService.getBusinessDateByAccentity(accentity, true, unSettAccountCheck);
                    if (Objects.isNull(accountInfoVo)) {
                        continue;
                    }
                    if (Objects.nonNull(accountInfoVo.getCostEndAccountDate()) && BizUtils.getCommpareDateYMD(accountInfoVo.getCostEndAccountDate(), businessDate) > 0) {
                        orderBusinessDateDto.setCode("999");
                        String message = InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_217876BA04600009", "操作失败, 业务日期不能小于") + accountInfoVo.getCostEndAccountDate();
                        if (Objects.nonNull(accountInfoVo.getBeginAccounted()) && accountInfoVo.getBeginAccounted().equals("1")) {
                            message = InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_217876DC05600008", "操作失败，产品成本期初余额首期已审核, 业务日期不能小于") + accountInfoVo.getCostEndAccountDate();
                        } else if (!unSettAccountCheck) {
                            message = InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_217876F804600004", "操作失败，产品成本已结账，业务日期不能小于") + accountInfoVo.getCostEndAccountDate();
                        }
                        orderBusinessDateDto.setMessage(message);
                    }
                }
            }
            return orderBusinessDateDtos;
        } catch (Exception ex) {
            throw new BusinessException("032-502-200073", com.yonyou.iuap.ucf.common.i18n.InternationalUtils.getMessageWithDefault("UID:P_FIEPCM-BE_1FF44A2E04F8005D", "根据会计主体查询成本关账日期出现异常！") /* "根据会计主体查询成本关账日期出现异常！" */, ex);
        }
    }


    private List<CheckLedgerDataVO> queryLedgerData(List<ProcessAccountDTO> processAccountDtos, List<RealCoOrderMapBO> orderMapBos) throws Exception {
        List<String> accentityIds = processAccountDtos.stream().map(ProcessAccountDTO::getAccentity).collect(Collectors.toList());
        List<AccInfoVO> accInfoVos = accInfoRepository.batchQueryCurrentPeriods(accentityIds);
        Map<String, List<AccInfoVO>> accInfoMap = accInfoVos.stream().collect(Collectors.groupingBy(AccInfoVO::getAccentity));
        Map<Long, List<RealCoOrderMapBO>> orderMap = orderMapBos.stream().collect(Collectors.groupingBy(RealCoOrderMapBO::getOrderId));
        List<CheckLedgerDataVO> checkLedgerDataVos = new ArrayList<>(orderMapBos.size());

        for (ProcessAccountDTO processAccountDto : processAccountDtos) {
            Long orderId = processAccountDto.getOrderId();
            String accentity = processAccountDto.getAccentity();

            List<AccInfoVO> datas = accInfoMap.get(accentity);
            if (CollectionUtils.isEmpty(datas)) {
                continue;
            }
            List<RealCoOrderMapBO> realCoOrderMapBos = orderMap.get(orderId);
            if (CollectionUtils.isEmpty(realCoOrderMapBos)) {
                continue;
            }
            realCoOrderMapBos.forEach(i -> datas.forEach(m -> {
                CheckLedgerDataVO vo = new CheckLedgerDataVO();
                vo.setAccentity(m.getAccentity());
                vo.setAccbook(m.getAccbook());
                vo.setAccYear(m.getPeriodYear());
                vo.setAccYearCode(m.getPeriodYearCode());
                vo.setPeriod(m.getPeriod());
                vo.setPeriodCode(m.getPeriodCode());
                vo.setProductCo(i.getRealCo());
                checkLedgerDataVos.add(vo);
            }));
        }
        return costCenterVoucherAdapter.checkLedgerIsHavingData(checkLedgerDataVos);
    }

    private Map<Long, Map<Boolean, String>> checkOrderIsHappen(List<RealCoOrderMapBO> orderMapBos, List<ProduceTotalQua> produceTotalQuaList, List<RealWkTimeBO> realWkTimeBos,
                                                               List<CheckLedgerDataVO> ledgerDataVos) {
        Map<Long, Map<Boolean, String>> resultMap = new HashMap<>();
        Map<String, List<ProduceTotalQua>> produceTotalQuaMap = produceTotalQuaList.stream().collect(Collectors.groupingBy(ProduceTotalQua::getRealco));
        Map<String, List<RealWkTimeBO>> realWkTimeMap = realWkTimeBos.stream().collect(Collectors.groupingBy(RealWkTimeBO::getRealco));
        Map<String, List<CheckLedgerDataVO>> ledgerMap = ledgerDataVos.stream().collect(Collectors.groupingBy(CheckLedgerDataVO::getProductCo));


        Map<Long, List<RealCoOrderMapBO>> orderMap = orderMapBos.stream().collect(Collectors.groupingBy(RealCoOrderMapBO::getOrderId));
        Set<Long> keys = orderMap.keySet();

        for (Long orderId : keys) {
            List<RealCoOrderMapBO> realCoOrderMapBos = orderMap.get(orderId);
            Map<Boolean, String> orderIsHappenMap = new HashMap<>();
            orderIsHappenMap.put(false, "");
            StringBuilder msg = new StringBuilder();
            for (RealCoOrderMapBO orderMapBo : realCoOrderMapBos) {
                String realCoId = orderMapBo.getRealCo();
                // 校验工序产量数据
                checkProductQuaIsHappen(realCoId, produceTotalQuaMap, msg);
                // 校验工时数据
                checkRealWKTimeIsHappen(realCoId, realWkTimeMap, msg);
                // 校验明细账
                if (msg.indexOf(EPCMBaseDocErrorCode.ERROR_141.getMessageFormat()) == -1 && CollectionUtils.isNotEmpty(ledgerMap.get(realCoId)) && ledgerMap.get(realCoId).get(0).getIsPresent()) {
                    if (msg.length() > 0) {
                        msg.append(",");
                    }
                    msg.append(InternationalUtils.getMessageWithDefault(EPCMBaseDocErrorCode.ERROR_141.getI18nCode(), EPCMBaseDocErrorCode.ERROR_141.getMessageFormat()));
                }
            }
            if (msg.length() > 0) {
                msg.append(InternationalUtils.getMessageWithDefault(EPCMBaseDocErrorCode.ERROR_142.getI18nCode(), EPCMBaseDocErrorCode.ERROR_142.getMessageFormat()));
                orderIsHappenMap.clear();
                orderIsHappenMap.put(true, msg.toString());
            }
            resultMap.put(orderId, orderIsHappenMap);
        }

        return resultMap;
    }

    private Map<String, Map<Boolean, String>> checkOrderIsHappen2(List<RealCoOrderMapBO> orderMapBos, List<ProduceTotalQua> produceTotalQuaList, List<RealWkTimeBO> realWkTimeBos,
                                                                  List<CheckLedgerDataVO> ledgerDataVos) {
        Map<String, Map<Boolean, String>> resultMap = new HashMap<>();
        Map<String, List<ProduceTotalQua>> produceTotalQuaMap = produceTotalQuaList.stream().collect(Collectors.groupingBy(ProduceTotalQua::getRealco));
        Map<String, List<RealWkTimeBO>> realWkTimeMap = realWkTimeBos.stream().collect(Collectors.groupingBy(RealWkTimeBO::getRealco));
        Map<String, List<CheckLedgerDataVO>> ledgerMap = ledgerDataVos.stream().collect(Collectors.groupingBy(CheckLedgerDataVO::getProductCo));


        Map<String, RealCoOrderMapBO> orderRowIdOpSnMap = orderMapBos.stream().collect(Collectors.toMap(item -> item.getRealCo(), item -> item, (key1, key2) -> key2));

        Set<String> keys = orderRowIdOpSnMap.keySet();

        for (String realCo : keys) {
            RealCoOrderMapBO realCoOrderMapBos = orderRowIdOpSnMap.get(realCo);
            Map<Boolean, String> orderIsHappenMap = new HashMap<>();
            orderIsHappenMap.put(false, "");
            StringBuilder msg = new StringBuilder();
            String realCoId = realCoOrderMapBos.getRealCo();
            // 校验工序产量数据
            checkProductQuaIsHappen(realCoId, produceTotalQuaMap, msg);
            // 校验工时数据
            checkRealWKTimeIsHappen(realCoId, realWkTimeMap, msg);
            // 校验明细账
            if (msg.indexOf(EPCMBaseDocErrorCode.ERROR_141.getMessageFormat()) == -1 && CollectionUtils.isNotEmpty(ledgerMap.get(realCoId)) && ledgerMap.get(realCoId).get(0).getIsPresent()) {
                if (msg.length() > 0) {
                    msg.append(",");
                }
                msg.append(InternationalUtils.getMessageWithDefault(EPCMBaseDocErrorCode.ERROR_141.getI18nCode(), EPCMBaseDocErrorCode.ERROR_141.getMessageFormat()));
            }
            if (msg.length() > 0) {
                msg.append(InternationalUtils.getMessageWithDefault(EPCMBaseDocErrorCode.ERROR_142.getI18nCode(), EPCMBaseDocErrorCode.ERROR_142.getMessageFormat()));
                orderIsHappenMap.clear();
                orderIsHappenMap.put(true, msg.toString());
            }
            resultMap.put(realCoOrderMapBos.getOrderRowId() + "_" + realCoOrderMapBos.getOpSn(), orderIsHappenMap);
        }

        return resultMap;
    }


    private void checkProductQuaIsHappen(String realCoId, Map<String, List<ProduceTotalQua>> produceTotalQuaMap, StringBuilder msg) {
        if (msg.indexOf(EPCMBaseDocErrorCode.ERROR_143.getMessageFormat()) != -1) {
            return;
        }
        List<ProduceTotalQua> produceTotalQuaList = produceTotalQuaMap.get(realCoId);
        if (CollectionUtils.isEmpty(produceTotalQuaList)) {
            return;
        }
        for (ProduceTotalQua produceTotalQua : produceTotalQuaList) {
            BigDecimal totalinqua = produceTotalQua.getTotalinqua();//累计投产数量
            BigDecimal totalfinqua = produceTotalQua.getTotalfinqua();//累计完工数量
            if ((Objects.nonNull(totalinqua) && totalinqua.compareTo(BigDecimal.ZERO) > 0) || (Objects.nonNull(totalfinqua) && totalfinqua.compareTo(BigDecimal.ZERO) > 0)) {
                msg.append(InternationalUtils.getMessageWithDefault(EPCMBaseDocErrorCode.ERROR_143.getI18nCode(), EPCMBaseDocErrorCode.ERROR_143.getMessageFormat()));
                break;
            }
        }
    }

    private void checkRealWKTimeIsHappen(String realCoId, Map<String, List<RealWkTimeBO>> realWkTimeMap, StringBuilder msg) {
        if (msg.indexOf(EPCMBaseDocErrorCode.ERROR_144.getMessageFormat()) != -1) {
            return;
        }
        List<RealWkTimeBO> realWkTimeBos = realWkTimeMap.get(realCoId);
        if (CollectionUtils.isEmpty(realWkTimeBos)) {
            return;
        }
        for (RealWkTimeBO realWkTimeBo : realWkTimeBos) {
            BigDecimal wktimequa = realWkTimeBo.getWktimequa();//本期投入工时
            BigDecimal finwktimequa = realWkTimeBo.getFinwktimequa();//本期完工工时
            BigDecimal totallabortimequa = realWkTimeBo.getTotallabortimequa();//累计完工工时_人工
            BigDecimal totalmachinetimequa = realWkTimeBo.getTotalmachinetimequa();//累计完工工时_机器
            BigDecimal totalothertimequa = realWkTimeBo.getTotalothertimequa();//累计完工工时_其它
            if ((Objects.nonNull(finwktimequa) && finwktimequa.compareTo(BigDecimal.ZERO) > 0) || (Objects.nonNull(wktimequa) && wktimequa.compareTo(BigDecimal.ZERO) > 0)
                    || (Objects.nonNull(totallabortimequa) && totallabortimequa.compareTo(BigDecimal.ZERO) > 0) || (Objects.nonNull(totalmachinetimequa) && totalmachinetimequa.compareTo(BigDecimal.ZERO) > 0)
                    || (Objects.nonNull(totalothertimequa) && totalothertimequa.compareTo(BigDecimal.ZERO) > 0)) {
                if (msg.length() > 0) {
                    msg.append(",");
                }
                msg.append(InternationalUtils.getMessageWithDefault(EPCMBaseDocErrorCode.ERROR_144.getI18nCode(), EPCMBaseDocErrorCode.ERROR_144.getMessageFormat()));
                break;
            }
        }
    }

    private List<RealCoOrderVO> pageResultData(List<Long> orderRowIds, EPCMBaseDocErrorCode errorCode) {
        List<RealCoOrderVO> result = new ArrayList<>();
        for (Long orderRowId : orderRowIds) {
            RealCoOrderVO vo = new RealCoOrderVO();
            vo.setOrderRowId(orderRowId);
            vo.setMeetChange(false);
            vo.setMessage(InternationalUtils.getMessageWithDefault(errorCode.getI18nCode(), errorCode.getMessageFormat()));
            result.add(vo);
        }
        return result;
    }

    private PeriodRealCoBO createPeriodRealCoBO(AccInfoVO accInfoVO, String realCoId, String orderCode, String periodCode) {
        PeriodRealCoBO periodRealCo = new PeriodRealCoBO();
        periodRealCo.setId(Long.toString(IdManager.getInstance().nextId()));
        periodRealCo.setRealCo(realCoId);
        periodRealCo.setOrderCode(FieldConsts.batchFinancialOpen);
        periodRealCo.setStartPeriodCode(periodCode);
        periodRealCo.setStartPeriodId(accInfoVO.getPeriod());
        periodRealCo.setAccentity(accInfoVO.getAccentity());
        periodRealCo.setAccpurpose(accInfoVO.getAccpurpose());
        periodRealCo.setAccbook(accInfoVO.getAccbook());
        periodRealCo.setControlscope(accInfoVO.getControlscope());
        periodRealCo.setAccClosed(EnumAccClosedStatus.AS_NOCLOSE.getValue());
        return periodRealCo;
    }
}
