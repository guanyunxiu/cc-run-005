const { CRDTDoc } = require('../public/crdt');
let pass=0, fail=0;
function ok(c,n){ if(c){pass++;console.log('  ✔',n);} else {fail++;console.log('  ✘',n);} }
const clone = o => JSON.parse(JSON.stringify(o));
const stroke = (x) => ({color:'#000',width:0.01,points:[{x,y:x}]});

// 1. 同锚点并发
{
  const A=new CRDTDoc('A'), B=new CRDTDoc('B'), S=new CRDTDoc('srv');
  const oa=A.localAdd(stroke(1)), ob=B.localAdd(stroke(2));
  S.receive(clone(oa)); S.receive(clone(ob));
  const d1=new CRDTDoc('X'); d1.loadSnapshot(S.snapshot());
  const d2=new CRDTDoc('Y'); d2.receive(clone(ob)); d2.receive(clone(oa));
  ok(d1.stateDigest()===d2.stateDigest(),'并发同锚插入：快照序与实时乱序收敛');
  ok(S.order().map(n=>n.id.clientId).join('')==='BA','tie-break 同 lamport → B 在 A 前');
}
// 2. 因果追加
{
  const A=new CRDTDoc('A'), B=new CRDTDoc('B');
  const oa=A.localAdd(stroke(1)); B.receive(clone(oa));
  const ob=B.localAdd(stroke(2));
  ok(B.order().map(n=>n.id.clientId).join('')==='AB','因果追加 A→B');
  ok(ob.id.lamport===3,'B 的 lamport = max(1,1)+1+1 = 3');
}
// 3. 乱序 pending + 级联 merged
{
  const A=new CRDTDoc('A'), C=new CRDTDoc('C');
  const oa1=A.localAdd(stroke(1));
  const tmp=new CRDTDoc('B'); tmp.receive(clone(oa1));
  const oa2=A.localAdd(stroke(2));
  const r2=C.receive(clone(oa2));
  ok(r2.status==='pending' && C.pending.length===1,'oa2 先到 → pending');
  const r1=C.receive(clone(oa1));
  ok(r1.status==='delivered' && r1.merged.length===2 && C.pending.length===0,'补齐 oa1 → 返回级联 2 条，pending 清空');
  ok(r1.merged.map(o=>o.id.lamport).join(',')==='1,2','级联按因果顺序 [1,2]');
  ok(C.order().length===2,'文档 2 节点');
}
// 4. 并发 add+del
{
  const A=new CRDTDoc('A'), B=new CRDTDoc('B');
  const x=A.localAdd(stroke(1)); B.receive(clone(x));
  const del=A.localDelete(x.id);
  const y=B.localAdd(stroke(2));
  const S1=new CRDTDoc('s1'); [del,y,x].forEach(o=>S1.receive(clone(o)));
  const S2=new CRDTDoc('s2'); [x,y,del].forEach(o=>S2.receive(clone(o)));
  const S3=new CRDTDoc('s3'); [y,del,x].forEach(o=>S3.receive(clone(o)));
  ok(S1.stateDigest()===S2.stateDigest() && S2.stateDigest()===S3.stateDigest(),'并发 del+add：3 种交错顺序收敛');
  ok(S1.tombstoneCount()===1 && S1.visibleStrokes().length===1,'1 墓碑 1 可见');
}
// 5. 幂等
{
  const A=new CRDTDoc('A'), C=new CRDTDoc('C');
  const oa=A.localAdd(stroke(1));
  C.receive(clone(oa));
  ok(C.receive(clone(oa)).status==='duplicate','重复 → duplicate');
  C.loadSnapshot({vv:A.vv,ops:[clone(oa)]});
  ok(C.nodes.size===1,'快照幂等不重复');
}
// 6. 撤销不影响并发新笔
{
  const A=new CRDTDoc('A'),B=new CRDTDoc('B'),S=new CRDTDoc('s');
  const x=A.localAdd(stroke(1)); B.receive(clone(x));
  const u=A.localDelete(x.id), y=B.localAdd(stroke(2));
  [x,u,y].forEach(o=>S.receive(clone(o)));
  const v=S.visibleStrokes();
  ok(v.length===1 && v[0].id.clientId==='B','撤销 A 的笔，B 并发新笔仍可见');
}
// 7. join 快照后继续增量
{
  const A=new CRDTDoc('A'),S=new CRDTDoc('s');
  const o1=A.localAdd(stroke(1)),o2=A.localAdd(stroke(2));
  S.receive(clone(o1));S.receive(clone(o2));
  const J=new CRDTDoc('J'); J.loadSnapshot(S.snapshot());
  const o3=A.localAdd(stroke(3)); S.receive(clone(o3));
  ok(J.receive(clone(o3)).status==='delivered','快照后增量 op 可交付');
  ok(J.order().length===3,'共 3 笔');
}
// 8. VV 因果判定：依赖未满足
{
  const A=new CRDTDoc('A'),B=new CRDTDoc('B'),C=new CRDTDoc('C');
  const a1=A.localAdd(stroke(1));
  B.receive(clone(a1));
  const b1=B.localAdd(stroke(2)); // vv {A:1,B:1}
  ok(C.receive(clone(b1)).status==='pending','b1 依赖 a1，未满足 → pending');
  C.receive(clone(a1));
  ok(C.pending.length===0 && C.order().length===2,'补 a1 后 b1 级联交付');
}
console.log(fail===0 ? '\n单元自检全部通过 ✅ ('+pass+')\n' : '\n'+fail+' 项失败 ❌\n');
process.exit(fail?1:0);
