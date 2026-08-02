CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
insert into t1 values ('placeholder', 'a', 'b');
insert into t1 values ('placeholder', '膨', 'p');
insert into t1 values ('placeholder', 'a', '膨');
insert into t1 values ('placeholder', 'a', '膨');
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1 VALUES
    ('a|b|c', 'ab', '123'),
    ('a|b|c', 'abc', '12'),
    ('C|S', 'CS', '测试'),
    ('测|试', '测试', 'CS'),
    ('测|试', '测试', 'C');
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1 SELECT CONCAT('a|b|c', generate_series), 'ab', '123' FROM TABLE(generate_series(1, 4095*2));
INSERT INTO t1 SELECT CONCAT('a|b|c', generate_series), 'abc', '12' FROM TABLE(generate_series(1, 4095*2));
INSERT INTO t1 SELECT CONCAT('C|S', generate_series), 'CS', '测试' FROM TABLE(generate_series(1, 4095*2));
INSERT INTO t1 SELECT CONCAT('测|试', generate_series), '测试', 'CS' FROM TABLE(generate_series(1, 4095*2));
INSERT INTO t1 SELECT CONCAT('测|试', generate_series), '测试', 'C' FROM TABLE(generate_series(1, 4095*2));
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1 VALUES
    ('abc', 'ab', ''),
    ('', 'CS', '测试');
INSERT INTO t1 VALUES ('abc', '', 'ab');
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1 VALUES
    ('abc', 'ab', NULL),
    (NULL, 'CS', '测试'),
    ('abc', null, 'ab');
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1 VALUES
    ('abc', 'ab', NULL),
    (NULL, 'CS', '测试'),
    ('abc', null, 'ab');
CREATE TABLE `t1` (
  `src` varchar(65533) NULL COMMENT "",
  `from_str` varchar(65533) NULL COMMENT "",
  `to_str` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`src`)
DISTRIBUTED BY HASH(`src`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
INSERT INTO t1 VALUES 
    ('ba1+E^]-', NULL, 'EE1aaababb'),
    ('(2%&j四[iH%û一G*%ù😂cûg˜', '%2(û*û😍😂在四', 'HjiiiG22'),
    ('🙂', 'K🙂🙂🙂🙂🙂🙂测🙂🙂🙂', '0wCnAcyybBK'),
    ('八*二四试DA%$aF[?^â%一›*Ji›a七+(cJ五^😄D三gûæ五', 'VFaiAgAJFcDu', 'DFJAFgF'),
    ('E?jjj?FE4二&3#$]æ#七二#eh三B*e^试4B˜E一(E', 'B*›æe?&', '七#eE*A?d'),
    ('*›Jù2âd0g-ga(#[di八%七🙂四%aH九âHcA--五I0›', 'i%-0-›cg四q0*eJ', 'Hcgk4gisgHHaaa'),
    ('ûbe', 'ûûbeeûBeûb', 'bûege在beQeeVx'),
    ('七0三$ei+DBgi测ùc(C试i˜试#)0+😄^)😄]e😇c4F八1G)三七ùHg🙂', 'K34Fc2E44e', 'B1e4HFgJDw'),
    (')g六BæùhähI)JB五j二', 'hJæ😍五âCBIBJ', 'JBDhBgvBI9Jeh'),
    ('HDa$四0d八测%😇æ)J一c?0)2测九jdaûf›二Câ', '九测)😇九aâ', 'a0dcad23DJKb'),
    ('Id二?-😇ba二Bg一f九E测E一(😇C测3JB?^2j😇A九&[+3JCh😂五五›', '😇J😇一-[Ej', 'Eb(+hg二'),
    ('九HB2😄六D🙂C七试三D五d3?D😇JùFA3I^?2#fa😇试😇七›dJ(b二iDb', 'F三f😇DJ试六xA2不J九', 'bobJAD3IH'),
    ('六G3一🙂a#', 'aG3G3F3a34', 'boaG3G'),
    ('七四c*û😂0七›测ââ2[三g]f*五Eä[Di测ù&2ä八-一', '四iâYNEù2', '20cC0cW2iE'),
    ('[h试û]八3六五i4Hb1äJ🙂😇🙂DB六🙂Efa八$f*0四i]Hâj1二G?一?I测😂', 'Hfh3r43JEnhD9I', 'fwhIGb'),
    ('1ù)一fC🙂G🙂Bd*😂^C$AI]]j?23hH˜?😄E)Cb1Ah4D3%😇三🙂]*â二û*', 'CIyjA31zGIZ', NULL),
    ('*试Jû😇三😇Ea四一+æ🙂G$F+›二八+🙂🙂😂😄cA#a#?c&äI四f˜iiæ试›ej😄🙂c', 'tceTni55ie', 'AGAFceF'),
    ('g-$一A˜g七afD+😂)一e^]ù😇Ah五D😇G*jI+试二G四#😇八#F&[2$0', 'WGuVZ4vG2DDDg', 'g0hAhe'),
    ('-D&i😇😇七Bù?#gä[G&😇', NULL, 'ä😇?😇&测ù不&D'),
    ('🙂四dc测äF^$))g4B😇äd4gG^eAaF😇九4G[I$]›Ib八-0七五五+😂#', '4Gbdg44IIAFgd', 'cSdIbqBHeAGBb'),
    ('😇😄试九äæ1七]A[J🙂七^五2g六😄ûû?3jf1e4?a😂ù🙂一Ed😇BfäI[j😇0e', 'g4LI😍J😇ä1😄🙂😂测a', 'f1测f😇û'),
    ('-F%âI4🙂六af', '六âqF%âII', '34jQ8IIIII'),
    ('A三e%😂›+(*三A试&一h%dä一ä&^cæj🙂五››ha(Câ四*Gû]#a??😇', 'd›h一😄ä›2', 'AG9ZjGIAePda'),
    ('D一#)', 'DsDDgDDbJDDZdD', 'zDDtDDD'),
    (NULL, '不😍1U˜试Ni', '9JLv在试😍0V😂˜x'),
    ('😂â😇2)#?1aA2ä五?测Gf˜æ%2%😇', 'GeG2Cf21a', 'G122pid2'),
    ('😄#2%六B😄[3)*gæ(A)试A[ä˜五^*😂三?😂二aF?äC八û0aI[F测4六æ&1', 'F14B1a', 'FFI52FAFAF10ag'),
    ('%›1Ga😂', 'NGGb11', 'aGaaa1'),
    ('(五û试%H试GH三ä四九3ah😇三(&âbâFib(Bâä试Iai[九4六3)â', '六âH试不âa三˜Bû(九九', 'âK(iFG[GH六Bi😇'),
    (NULL, NULL, 'QJngp57'),
    ('😂[六›˜&六三^]âE2A四0âb二JFf?û[六3^-d🙂˜dä', 'd六û˜câ😂d-六s[', 'E3s03yd'),
    ('+', 'BnloXjep1Vosfz', 'asJO5T'),
    ('#bC3*六八?**J试a七)äBi😇E试d😂]五九(˜)-E)😇六i4+一-Cd]2九七', 'diCdJEB', '›4a九测˜-在-b'),
    ('1[a-˜Fbiæj', 'ba1b1j', 'VFâF˜1j[b'),
    ('测G', 'GGEGGG', '😍qGn测测G测GG6不G'),
    ('*j)😂(F-B#â+😇d3测H#三五试(ù0e+f八七[äI#f˜G4A#', '63O0fnds3', 'IffFddRFjI34'),
    ('六c八0)GâûFgCä一i›gF', 'iCFcHGd', 'iCwcGgFggP'),
    ('f😂三-&â)J4', 'hJffJfIBfJJ', '-â&â不试e)&'),
    ('三e*FaE$4e%测?C#二JæHc]四六+)九B˜四æj››😇😄一gIfgdDG七ù4', '4😂g😄R$不D', NULL),
    ('九g九%😂(0九b七›j)fi试F😇八😄[一0ùE›F八3ûûhc3测八+äe', 'a%Xnf😂', 'hF0giZE0e3'),
    ('e二d*D%4eäcæ*û😂*', '在ä测û在在eBb8e*æ', 'e4cc4Ddge'),
    (NULL, '8VjLoB8', 'bDNPgO3f'),
    ('Câ测六三A😇😂Ggc一+1›f21j2%f😇â五æaBj*-#C六C', '1AcGcB1QICBjG', 'g😇j😂›GH˜六o'),
    ('hE九H一cC测测cj2%', 'EEEcC1cczmHr', 'jEHHhDj'),
    ('', 'jqIPRsQc', '3S4KSpB'),
    ('bH二ùD八J😂j2b八2😂B-ghä#&😂D+Hdûf😂e测', 'hBDE1HSjJHHJj', 'U2bjmbb8dhI2'),
    ('[😇二0C试ä', '01CC00CCaC', '0CHCCC0CC0CCvC'),
    ('😄D1˜九C八0六三jea-e3&e试e', 'et2J03G', 'eUok00jCDDGa'),
    (NULL, '›Y测l😜fâN', 'D不âmâ6e😜'),
    ('#]九â*JCg)jû^äHùAi四Jf#3', 'ACfgj3jhUC', '#ä)ä九J'),
    ('?hA七🙂HI二#0试😇(四七ùI+a˜ä]二fEb九H12', '2I20hAbOEF', 'hHGI4TAaH'),
    ('Fc˜D4二八Fj😂4i三2😄六二四E34h%g九%D24˜&七Bdedjj😂😂a1试二', NULL, 'v3x%2😂437试&D'),
    ('%I八五四JfJ六&Gc测E3D七+H1e0#Jâäe', 'eNJfgIHEJ', 'ä测0I3JJ七四'),
    ('六#i)˜äigù2DC32c1â六3gd九J3b五$1😂1😇🙂3九æBd^七J%a', '^)S2bd', '33B29五A九dC'),
    ('fE六fb二)I七3?[*😂0G#JA🙂BG3J五测测', 'IfEJGEJJBJbG3b', '?*3J😂二G'),
    ('0😄c-三g😂E2â+-)😂â二#C', 'ä-c2😂˜OCâm', '二â#😂#2-g😂a2三'),
    ('aäAgCæ4一˜😂B4测3æg-3û1三æ›H#?', '43A4HAHa3', '1三?-1F›测一g一'),
    ('If测七Fi4ùFc', 'mIfif4FiFF测i😂', 'fFFF4iaI'),
    ('😄H[五A+试1c🙂J七I六bä+E😇[', '1HwccAJ1EJo', 'HbAbAA'),
    ('æ三测ûfi][)42û˜jd3i1i?gcI😂(fD[e?FI', 'Icddie', 'ibfgfiij3Dfbi'),
    ('D*^D八😂1😄测H4八Df九ebe4', '😄f*fâDDf😂›😂九f', '4D14s1eH6D4b'),
    ('八c-0e九g3二二a二i八igJ七e', 'kJcJ二e二ig二m', 'caaMgigdiyJF'),
    ('D#GD^4九试cE[八?-j0F03D', 'w试-DE-pb-[E试9#', '03LDF3D'),
    ('三%-&一û二F', 'FFPFFFFF', 'FFFFFsQw91'),
    ('Ji三æG0(0dùûh六[G^七3]八2', 'Ge0i0Fn0h3', 'GGG303'),
    ('jC*八^八六ce›a(GâB', 'cC1GeePGC', 'a6GjcC'),
    ('三3一四â4˜', '43G3333344I4', '😍四一˜ââ34一˜😜3'),
    ('八😄hbEdG一H?😇gFù🙂🙂+1û*五æ(😄D', 'EGgdZbFyEbE', 'GQgX1hFEghDHE'),
    ('😂FFj🙂D%D?一ùùûä0fi2ba(A#a*fû', 'bAffaojiDfU2i7', '?m2˜%😍D0anâfù'),
    ('3*i八I八(五j-🙂?', 'W八😄?八IIi?八i🙂', '3ULIIPu'),
    ('fhI二hIf˜âaC三', 'fC˜˜I二˜IhaGq', '😜˜Ehâa三hä'),
    ('Gæj1^', 'G1jGy111j', 'j3QG1jG'),
    ('😂0C%🙂$?äi😄六h三测^🙂0Eù0))æIæ›h0i七i-4)3B›E#4😄', ')hæh😄😄BEæ?', 'ih4Ei0I0'),
    ('˜四I*CEg1🙂]+', 'CCCgIgIC', 'EsECCIC2111'),
    ('c0C😇0😄', NULL, '0CJp0c00V0'),
    ('4%?j测[])0FGBg]六九db九i*四四I😄九😄', 'oZ测九Q九B', 'biGFVbRbgB'),
    ('äda1五4$&0I#😄😄ä五$试测$🙂I测æG七›æ😄😇', 'IIIIG11pdG1', 'dG1ddd'),
    ('^]?测äc🙂e七dbcû😄f4â😇+*I)+û测Bd八😇四1', '4beB1c1IdfIQ1b', 'bbdRIf'),
    ('三g二😄测[-😇*', 'gggHggggogg', 'ggggIgrgggg'),
    ('2IæIGj😇[(^I+G一B*c-hâ九😇00八jC', 'CCGxhI', 'cGG😜J˜It0在j一'),
    ('4›ää0😄2测IJDe', 'ZDo4D4e', '😄😂不😄I›😄4'),
    ('jj(f七[e', '不😜j七IAjFe七j(ej', 'jjjjfefjjfffew'),
    ('h$3%-CBæDeG八)›#三#I#&gc😇六Ii$八-+e', 'D-#+😂#DI😜âæd', 'h7eBihgcDjgB'),
    ('BC七四?i-Bd3]', 'BC38di', 'B33B3BBdC3Y'),
    ('F(五#›H[七%H%四一九D›3😇e-😂九)1二D&ä', '😇1yF#九)(😂', 'UFR五😂C[二'),
    ('DjHGJ˜dBûa˜试H)&#d一â›0j›D', 'Hl试D)››H', NULL),
    ('C*#测›hâFc', 'Câ测Fc测oV›', 'CchFChchFhDcfF'),
    ('六*六bæ九gg[&三🙂(4C&H1^3›^d˜D%ù六测试4E一)六0ù', 'H[D4😂J三一六在九˜试', 'b4g1DggCp04'),
    ('🙂一八C五C', 'CrCCCC', 'ClCCCC'),
    ('eæE一û+I]3八ä试四?七试jfFBGB0â三四', '5EGo09En', '八++?FbG]一😂四WEf'),
    ('â😄0Fû', 'p0FFAF', 'D0â😄00a'),
    ('六🙂一测Bfcj0AaC九😇D七gG[1^八1九GCAe1æiB😇(🙂+D😄六c', 'ioY1cgD', '九B1G一c'),
    ('H五G六d试›ca(+?gC^ù$g[三e&Aj四😄ä', 'b五六四?03H+😄›三+', 'jHCSjAeecd1BH'),
    ('[$aâj二😄C]?ù八ûù[试æ˜四', 'ajDCgajaaj', 'û˜测d?â测æùj试'),
    ('GCcf+i五31🙂🙂八?)˜˜d😇$九试ûe0+æ1🙂#', '1aG🙂?)', 'C˜E🙂äCCB不$五🙂试'),
    ('^›测#]J)ihâI九2九八2^J-)˜3四1😇1B1g二&B二gj', 'lggjB2j63gh', '))˜1B3]J-ji&J'),
    (NULL, '˜q在不GN在X测', '˜在âMBy😄6'),
    ('', 'QTL3ND', 'UTs1QEhpnf'),
    ('iEJG2A六0AB#(', 'BE六AAGG(六', 'EiGAi0GigA'),
    (')i-五二+$Bc+六â&三四^eG*七1ûcæ0äF?J八(*六D四五û九%CfJ*C九â', 'J四%O(测三B0', NULL);