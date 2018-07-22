#!/usr/bin/env ruby
# encoding: utf-8

module TastySpleen

  class UMerge; end

  module UMerge::RowConstants
    IDX_NAME  = 0
    IDX_UID   = 2
    IDX_UGID  = 3
    IDX_GID   = 2
  end # UMerge::RowConstants
  
  class UMerge::UData
    include Enumerable
    include TastySpleen::UMerge::RowConstants
    
    def initialize
      @data = []
    end
    
    def load(path)
      @data = []
# warn "load: path=#{path.inspect}"
      IO.foreach(path) do |line|
        line.chomp!
        next if line.strip.empty?
        fields = line.split(/:/, -1)
        @data << fields
      end
      self
    end
    
    def save(path)
      File.open(path, "w") do |io|
        @data.each do |row|
          io.puts(row.join(":"))
        end
      end
      self
    end
    
    def dup
      copy = self.class.allocate
      copy.instance_variable_set(:@data, @data.dup)
      copy
    end
    
    def size
      @data.size
    end
    
    def each
      @data.each {|row| yield row}
    end
    
    def <<(row)
      @data << row
    end
    
    def sort_by_ids!
      @data = @data.sort_by do |row|
        id = row[IDX_UID].to_i
        id = 99.9 if (id == 65534)  # hideous kludge so 'nobody/nogroup' stays just below 100 in the list (aesthetic reasons only)
        id
      end
    end
    
    def ids_to_integer!(row_idx)
      @data.each do |row|
        id = row[row_idx]
        Integer(id)  # sanity check (but it could convert to octal, so ignore result)
        id = id.to_i
        row[row_idx] = id
      end
    end
    
    def uids_to_integer!
      ids_to_integer!(IDX_UID)
    end
    
    alias gids_to_integer! uids_to_integer!
    
    def ugids_to_integer!
      ids_to_integer!(IDX_UGID)
    end
    
    def find_by_name(name)
      @data.find {|row| row[IDX_NAME] == name}
    end
    
    alias find_by_uname find_by_name
    alias find_by_gname find_by_name
    
    def find_by_id(id)
      @data.find {|row| (! (rid = row[IDX_UID]).kind_of?(Array)) && (rid == id)}
    end

    alias find_by_uid find_by_id
    alias find_by_gid find_by_id
    
    def find_available_uids(range_low, range_high)
      uids = (range_low..range_high).to_a
      self.each {|row| uids.delete(row[IDX_UID]) if row[IDX_UID]}
      uids
    end
    
    def build_name_to_id_map
      name_to_id = {}
      @data.each do |row|
        name, id = row[IDX_NAME], row[IDX_UID]
        (name_to_id.key?(name)) and raise("duplicate mapping for name #{name.inspect}")
        name_to_id[name] = id
      end      
      name_to_id
    end
    
    alias build_name_to_uid_map build_name_to_id_map
    alias build_name_to_gid_map build_name_to_id_map
    
    def build_gid_to_gname_map
      gid_to_gname = {}
      @data.each do |row|
        gname, gid = row[IDX_NAME], row[IDX_GID]
        (gid_to_gname.key?(gid)) and raise("duplicate mapping for gid #{gid.inspect}")
        gid_to_gname[gid] = gname
      end
      gid_to_gname
    end
    
    def build_gname_to_gid_map
      gname_to_gid = {}
      @data.each do |row|
        gname, gid = row[IDX_NAME], row[IDX_GID]
        (gname_to_gid.key?(gname)) and raise("duplicate mapping for gname #{gname.inspect}")
        gname_to_gid[gname] = gid
      end
      gname_to_gid
    end
  end # UMerge::UData

  class UMerge::DataSet
    UData = TastySpleen::UMerge::UData
    
    PASSWD_FNAME  = "passwd"
    SHADOW_FNAME  = "shadow"
    GROUP_FNAME   = "group"
    GSHADOW_FNAME = "gshadow"
    
    attr_reader :passwd, :shadow, :group, :gshadow
    
    def initialize
      @passwd, @shadow, @group, @gshadow = nil
    end
    
    def load(path)
      @passwd  = UData.new.load(File.join(path, PASSWD_FNAME))
      @shadow  = UData.new.load(File.join(path, SHADOW_FNAME))
      @group   = UData.new.load(File.join(path, GROUP_FNAME))
      @gshadow = UData.new.load(File.join(path, GSHADOW_FNAME))
      @passwd.uids_to_integer!
      @passwd.ugids_to_integer!
      @group.gids_to_integer!
      self
    end
    
    def save(path)
      @passwd.save(File.join(path, PASSWD_FNAME))
      @shadow.save(File.join(path, SHADOW_FNAME))
      @group.save(File.join(path, GROUP_FNAME))
      @gshadow.save(File.join(path, GSHADOW_FNAME))
      self
    end
    
    def sort_by_ids!
      @passwd.sort_by_ids!
      @group.sort_by_ids!
    end
  end # UMerge::DataSet
  

  class UMerge
    include RowConstants
    
    MergeState = Struct.new(:n_remaps, :collisions, :uremaps, :gremaps)
    
    def initialize
    end
    
    def new_merge_state
      MergeState.new(0, [], [], [])
    end
    
    def to_uremap_args(uremaps, gremaps)
      ur = uremaps.map {|name, id1, id2| "-u#{id1}:#{id2}"}
      gr = gremaps.map {|name, id1, id2| "-g#{id1}:#{id2}"}
      (ur + gr)
    end
    
    def merge(dest_dir, src_dir, dest_output_dir, src_output_dir, preferred_name_to_uid_generator=nil)
      st = new_merge_state()
      dest = DataSet.new.load(dest_dir)
# warn "\nmerge: dest_dir=#{dest_dir.inspect} dest=#{dest.inspect}"
      src  = DataSet.new.load(src_dir)
      remap_ugid_to_symbolic(dest.passwd, dest.group)
      remap_ugid_to_symbolic(src.passwd, src.group)
# warn "--- merge_udata --------------------------------------------------------"
      merge_udata(st, dest, src, preferred_name_to_uid_generator)
# warn "--- merge_gdata --------------------------------------------------------"
      merge_gdata(st, dest, src)
      remap_ugid_to_numeric(src.passwd, src.group)
      remap_ugid_to_numeric(dest.passwd, dest.group)
      dest.sort_by_ids!
      src.sort_by_ids!
      dest.save(dest_output_dir)
      src.save(src_output_dir)
      [st.uremaps, st.gremaps]
    end
    
    protected

    def gen_required_name_to_id(dest_rows, preferred_name_to_id, mtype, warn_no_preferred=true)
      required_name_to_id = dest_rows.build_name_to_id_map
      
      uniq_ids = {}
      required_name_to_id.each_pair do |name, id|
        (uniq_ids.key?(id)) and raise("ERROR: duplicate #{mtype}id #{id}")
        uniq_ids[id] = name
      end
      
      preferred_name_to_id.each_pair do |name, id|
        if required_name_to_id.key?(name)
          if (d_id = required_name_to_id[name]) != id
            warn("WARN: preferred #{mtype}id #{name}=>#{id} collides with dest #{name}=>#{d_id}, ignoring") if warn_no_preferred
          end
        else
          if uniq_ids.key?(id)
            warn("WARN: preferred #{mtype}id #{name}=>#{id} collides with dest #{uniq_ids[id]}=>#{id}, ignoring") if warn_no_preferred       
          else
            required_name_to_id[name] = id
            uniq_ids[id] = name
          end
        end
      end
      required_name_to_id
    end

    def merge_udata(st, dest, src, preferred_name_to_uid_generator)
      preferred_name_to_id = (preferred_name_to_uid_generator) ? preferred_name_to_uid_generator.call(src.passwd) : {}
      required_name_to_id = gen_required_name_to_id(dest.passwd, preferred_name_to_id, ?u)

# warn "\nmerge_udata: required_name_to_id=#{required_name_to_id.inspect}"

      merge_data(st, dest.passwd, dest.shadow, src.passwd, src.shadow, st.uremaps, ?u, required_name_to_id)
    end
    
    def merge_gdata(st, dest, src)
      preferred_name_to_id = dest.passwd.build_name_to_uid_map  # try to keep gids with uids
      
      warn_no_preferred = false  # non-preferred mappings are common here, as we're just trying to map gids to corresponding uids if possible
      required_name_to_id = gen_required_name_to_id(dest.group, preferred_name_to_id, ?g, warn_no_preferred)
    
# warn "\nmerge_gdata: required_name_to_id=#{required_name_to_id.inspect}"

      merge_data(st, dest.group, dest.gshadow, src.group, src.gshadow, st.gremaps, ?g, required_name_to_id)
    end

    SYSTEM_IDS_BANK_SIZE = 1000

    def merge_data(st, dest_db, dest_shadow, src_db, src_shadow, remaps, mtype, required_name_to_id)
      (required_name_to_id.values.size == required_name_to_id.values.uniq.size) or raise("ERROR: multiple names map to same value in required_name_to_id: #{required_name_to_id.inspect}")
      
      # Pass 1.
      #   Just remap all required src ids to required dest ids, if any.
      #   
      #   Any remaps that collide with existing ids in src, uh, get limboed
      #
      src_db.each do |row|
# warn  "row=#{row.inspect}"
        name, id = row[IDX_NAME], row[IDX_UID]
        
        next if (id.kind_of?(Numeric) && id.zero?)  # we don't mess with root
        
        # NOTE: <id> might be Array[id], if we limboed this row below
        
        if (r_id = required_name_to_id[name])
# warn "pass_1: required: #{id.inspect}=>#{r_id.inspect} #{row.inspect}"
          if id == r_id
            copy(row, src_shadow, dest_db, dest_shadow)  # checks if already in dest
          else
            # Limbo any possible row with our new id in <src_db>
            if (coll_row = src_db.find_by_id(r_id))
              coll_row[IDX_UID] = [coll_row[IDX_UID]]
# warn "LIMBOED: (#{mtype}ids) id=#{id} r_id=#{r_id} name=#{name} #{coll_row.inspect}"
            end
            
            orig_id = (id.kind_of?(Array)) ? id.first : id
            
            remap_and_copy(st, row, orig_id, r_id, src_db, dest_db, dest_shadow, remaps)
          end
        end
      end
      
      # Pass 2.
      #   Find homes for any rows with non-required ids.
      #
      src_db.each do |row|
        name, id = row[IDX_NAME], row[IDX_UID]
        
        next if ((id.kind_of?(Numeric) && id.zero?) || required_name_to_id[name])  # handled in pass 1
        
        (dest_db.find_by_name(name)) and raise("ERROR: unexpected: pass 2: name #{name.inspect} should not exist in dest, pass 1 should have handled all preexisting named rows (row=#{row.inspect})")
        
        # NOTE: <id> might be Array[id], if limboed above
        
        need_remap = (id.kind_of?(Array) || dest_db.find_by_id(id))
# warn "pass_2: need_remap=#{need_remap.inspect} #{row.inspect}"
        if need_remap
          id = id.first if id.kind_of?(Array)
          
          bsize = SYSTEM_IDS_BANK_SIZE
          bank = (id / bsize)
          id_range_low = bank * bsize
          id_range_high = (id_range_low + (bsize - 1))
          if bank.zero?
            # If we're in the system accounts bank, don't go lower
            # than original <id>.  (This is arbitrary, but it feels
            # weird to have a 103 map down to a 35, or whatnot.)
            id_range_low = id
          end
          new_id = find_mutually_available_id(src_db, dest_db, id_range_low, id_range_high)
          if new_id
            remap_and_copy(st, row, id, new_id, src_db, dest_db, dest_shadow, remaps)
          else
            raise "out of #{mtype}ids remapping #{id}"
          end
          
        else
          copy(row, src_shadow, dest_db, dest_shadow)
        end
      end
    end
    
    def remap_ugid_to_symbolic(passwd, group)
      gid_to_gname = group.build_gid_to_gname_map
      passwd.each do |row|
        ugid = row[IDX_UGID]
        gname = gid_to_gname[ugid]
        (ugid && gname) or raise("missing gid_to_gname map for ugid=#{ugid.inspect} gname=#{gname.inspect}")
        row[IDX_UGID] = gname
      end
    end
    
    def remap_ugid_to_numeric(passwd, group)
      gname_to_gid = group.build_gname_to_gid_map
      passwd.each do |row|
        gname = row[IDX_UGID]  # changed from ugid to gname by prior remap
        ugid = gname_to_gid[gname]
        (gname && ugid) or raise("missing gname_to_gid map for gname=#{gname.inspect} ugid=#{ugid.inspect} gname_to_gid=#{gname_to_gid.inspect}")
        row[IDX_UGID] = ugid
      end
    end

    def remap_and_copy(st, row, orig_id, new_id, src_shadow, dest_db, dest_shadow, remaps)
      name = row[IDX_NAME]
# warn "remap #{name} #{orig_id}=>#{new_id}"
      row[IDX_UID] = new_id
      remaps << [name, orig_id, new_id]
      st.n_remaps += 1
      copy(row, src_shadow, dest_db, dest_shadow)
    end
    
    def copy(row, src_shadow, dest_db, dest_shadow)
      name = row[IDX_NAME]
      if (d_row = dest_db.find_by_name(name))
        (d_row[IDX_UID] == row[IDX_UID]) or raise("ERROR: unexpected: adding #{name}=>#{row[IDX_UID]} to dest, but name with mismatching id already exists (row=#{row.inspect}, d_row=#{d_row.inspect})")
        # already in dest, nothing to copy
# warn "nocp: #{row.inspect}"
      else
        (! dest_db.find_by_id(row[IDX_UID])) or raise("ERROR: unexpected: adding #{name}=>#{row[IDX_UID]} to dest, but id already exists (row=#{row.inspect})")
# warn "copy: #{row.inspect}"
        dest_db << row.dup
        shadow_row = src_shadow.find_by_name(name)
        if shadow_row
          dest_shadow << shadow_row.dup
        end
      end
    end
    
    def find_mutually_available_id(p1, p2, uid_range_low, uid_range_high)
      uids1 = p1.find_available_uids(uid_range_low, uid_range_high)
      uids2 = p2.find_available_uids(uid_range_low, uid_range_high)
      uids = (uids1 & uids2)
      (uids.empty?) ? nil : uids.first
    end
    
  end # UMerge

end # TastySpleen


if $0 == __FILE__
  def usage_abort(msg=nil)
    warn(msg) if msg
    abort("Usage: #{File.basename($0)} srcdir_a srcdir_b dstdir_a dstdir_b [preferred_name_to_uid_module]")
  end

  (ARGV.size.between?(4,5)) or usage_abort
  
  srcdir_a = ARGV.shift
  srcdir_b = ARGV.shift
  dstdir_a = ARGV.shift
  dstdir_b = ARGV.shift
  
  pnu_module_path = (ARGV.empty?) ? nil : ARGV.shift
  
  [["srcdir_a", srcdir_a], ["srcdir_b", srcdir_b], ["dstdir_a", dstdir_a], ["dstdir_b", dstdir_b]].each do |label, path|
    test(?d, path) or usage_abort("FATAL: #{label} not found at #{path.inspect}")
  end
  
  pnu_proc = nil
  
  if pnu_module_path
    test(?f, pnu_module_path) or usage_abort("FATAL: preferred_name_to_uid_module not found at #{pnu_module_path.inspect}")
    
    pnu_proc = eval(File.read(pnu_module_path).strip)
    
    (pnu_proc && pnu_proc.respond_to?(:call)) or usage_abort("FATAL: preferred_name_to_uid_module generated unusable proc: #{pnu_proc.inspect}")
  end
  
  merger = TastySpleen::UMerge.new
  uremaps, gremaps = merger.merge(srcdir_a, srcdir_b, dstdir_a, dstdir_b, pnu_proc)
  
  remap_args = merger.to_uremap_args(uremaps, gremaps)
  
  File.open(File.join(dstdir_b, "uremap_argfile"), "w") {|io| io.puts(remap_args.join("\n"))}
end

