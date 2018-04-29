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
    
    def size
      @data.size
    end
    
    def each
      @data.each {|row| yield row}
    end
    
    def <<(row)
      @data << row
    end
    
    def find_by_name(name)
      @data.find {|row| row[IDX_NAME] == name}
    end
    
    alias find_by_uname find_by_name
    alias find_by_gname find_by_name
    
    def find_by_uid(uid)
      uid = Integer(uid)
      @data.find {|row| row[IDX_UID].to_i == uid}
    end

    def find_available_uids(range_low, range_high)
      uids = (range_low..range_high).to_a
      self.each {|row| uids.delete(row[IDX_UID].to_i) if row[IDX_UID]}
      uids
    end
    
    def build_name_to_id_map
      name_to_id = {}
      @data.each do |row|
        id = Integer(row[IDX_UID])
        name = row[IDX_NAME]
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
        gid = Integer(row[IDX_GID])
        gname = row[IDX_NAME]
        (gid_to_gname.key?(gid)) and raise("duplicate mapping for gid #{gid.inspect}")
        gid_to_gname[gid] = gname
      end
      gid_to_gname
    end
    
    def build_gname_to_gid_map
      gname_to_gid = {}
      @data.each do |row|
        gid = Integer(row[IDX_GID])
        gname = row[IDX_NAME]
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
      self
    end
    
    def save(path)
      @passwd.save(File.join(path, PASSWD_FNAME))
      @shadow.save(File.join(path, SHADOW_FNAME))
      @group.save(File.join(path, GROUP_FNAME))
      @gshadow.save(File.join(path, GSHADOW_FNAME))
      self
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
    
    def merge(dest_dir, src_dir, output_dir, preferred_name_to_uid_generator=nil)
      st = new_merge_state()
      dest = DataSet.new.load(dest_dir)
      src  = DataSet.new.load(src_dir)
      remap_ugid_to_symbolic(dest.passwd, dest.group)
      remap_ugid_to_symbolic(src.passwd, src.group)
      preferred_name_to_uid = (preferred_name_to_uid_generator) ? preferred_name_to_uid_generator.call(src.passwd) : {}
      merge_udata(st, dest, src, preferred_name_to_uid)
      merge_gdata(st, dest, src)
      remap_ugid_to_numeric(src.passwd, src.group)
      remap_ugid_to_numeric(dest.passwd, dest.group)
      dest.save(output_dir)
      [st.uremaps, st.gremaps]
    end
    
    def merge_udata(st, dest, src, preferred_name_to_uid={})
      merge_data(st, dest.passwd, dest.shadow, src.passwd, src.shadow, st.uremaps, preferred_name_to_uid)
    end
    
    def merge_gdata(st, dest, src)
      preferred_name_to_id = dest.passwd.build_name_to_uid_map  # try to keep gids with uids
      merge_data(st, dest.group, dest.gshadow, src.group, src.gshadow, st.gremaps, preferred_name_to_id)
    end

    def merge_data(st, dest_passwd, dest_shadow, src_passwd, src_shadow, remaps, preferred_name_to_id={})
      pass = 0
      begin
        pass += 1
        st.n_remaps = 0
        st.collisions.clear
# warn "\npass... dest_sz=#{dest_passwd.size} src_sz=#{src_passwd.size}"
        merge_data_pass(st, dest_passwd, dest_shadow, src_passwd, src_shadow, remaps, preferred_name_to_id, pass)
        done = (st.n_remaps.zero? && st.collisions.empty?) || (pass > (src_passwd.size * 2))
      end until done
      
      unless st.collisions.empty?
        raise "unresolvable collisions: #{st.collisions.inspect}"
      end
    end
    
    protected
    
    def remap_ugid_to_symbolic(passwd, group)
      gid_to_gname = group.build_gid_to_gname_map
      passwd.each do |row|
        ugid = Integer(row[IDX_UGID])
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
        (gname && ugid) or raise("missing gname_to_gid map for gname=#{gname.inspect} ugid=#{ugid.inspect}")
        row[IDX_UGID] = ugid
      end
    end
    
    # FIRST_USER_UID = 1000
    
    def merge_data_pass(st, dest_passwd, dest_shadow, src_passwd, src_shadow, remaps, preferred_name_to_id, pass)
      src_passwd.each do |row|
# warn  "row=#{row.inspect}"
        uname, uid = row[IDX_NAME], row[IDX_UID].to_i
        
        dest_row = dest_passwd.find_by_uname(uname)
        if dest_row
          dest_uid = dest_row[IDX_UID].to_i
          if uid == dest_uid
            # Already exists in dest with same uid, nothing to do.
          else
            # Name exists in dest, but uid mismatch.
            # If we remap, would there be a collision in *SRC*?
            coll_src_row = src_passwd.find_by_uid(dest_uid)
            if coll_src_row
              # If we tried to remap uid to dest_uid, we'd collide
              # with another uid already in src.
              coll_uname, coll_uid = coll_src_row[IDX_NAME], coll_src_row[IDX_UID].to_i
              st.collisions << "src(#{uname}:#{uid}=>#{coll_uname}:#{coll_uid})"
            else
              # Safe to remap.
              row[IDX_UID] = dest_uid
              remaps << [uname, uid, dest_uid]
              st.n_remaps += 1
            end
          end
        else
          # Name does not exist in dest yet.
          # Could we copy directly? Or is remap still needed to 
          # avoid collision?
          #
          # For first round of passes, report src-collision if we're blocked
          # trying to remap to preferred id. (First round of N passes could
          # shuffle ids out of the way.)
          coll_src_row = nil
          remap_to_preferred = (preferred_uid = preferred_name_to_id[uname]) && (uid != preferred_uid) &&
                                (! dest_passwd.find_by_uid(preferred_uid)) &&
                                ((! (coll_src_row = src_passwd.find_by_uid(preferred_uid))) || (pass <= src_passwd.size))
          
          if remap_to_preferred
            if coll_src_row
              # We'll get here if we're in the first round of N(=src.size) passes
              # and we hit a src=>src conflict trying to remap to preferred id.
              # (After that, we'll have given up on preferred id collision 
              # resolution.)
              coll_uname, coll_uid = coll_src_row[IDX_NAME], coll_src_row[IDX_UID].to_i
              st.collisions << "src(#{uname}:#{uid}=>#{coll_uname}:#{coll_uid})"
            else
              remap_and_copy(st, row, uname, uid, preferred_uid, src_shadow, dest_passwd, dest_shadow, remaps)
            end
          else
            (preferred_uid && (uid != preferred_uid)) && warn("WARN: failed to remap #{uname.inspect} to preferred_id #{preferred_uid.inspect}")
            
            coll_dest_row = dest_passwd.find_by_uid(uid)
            if coll_dest_row
              # Src uid already exists in dest under a different name.
              # We need to remap.
              # is_system_uid = (uid < FIRST_USER_UID)
              bank = (uid / 1000)
              uid_range_low = bank * 1000
              uid_range_high = (uid_range_low + 999)
              if bank.zero?
                # If we're in the system accounts bank, don't go lower
                # than original <uid>.  (This is arbitrary, but it feels
                # weird to have a 103 map down to a 35, or whatnot.)
                uid_range_low = uid
              end
              new_uid = find_mutually_available_uid(src_passwd, dest_passwd, uid_range_low, uid_range_high)
              if new_uid
                remap_and_copy(st, row, uname, uid, new_uid, src_shadow, dest_passwd, dest_shadow, remaps)
              else
                raise "out of uids remapping #{uid}"
              end
            else
              # No conflict. We can just copy.
              dest_passwd << row.dup
              shadow_row = src_shadow.find_by_uname(uname)
              if shadow_row
                dest_shadow << shadow_row.dup
              end
              st.n_remaps += 1  # not really a "remap", just an 'action'
            end
          end
        end
      end
    end
    
    def remap_and_copy(st, row, uname, orig_uid, new_uid, src_shadow, dest_passwd, dest_shadow, remaps)
      row[IDX_UID] = new_uid
      remaps << [uname, orig_uid, new_uid]
      st.n_remaps += 1
      dest_passwd << row.dup
      shadow_row = src_shadow.find_by_uname(uname)
      if shadow_row
        dest_shadow << shadow_row.dup
      end
    end
    
    def find_mutually_available_uid(p1, p2, uid_range_low, uid_range_high)
      uids1 = p1.find_available_uids(uid_range_low, uid_range_high)
      uids2 = p2.find_available_uids(uid_range_low, uid_range_high)
      uids = (uids1 & uids2)
      (uids.empty?) ? nil : uids.first
    end
    
  end # UMerge

end # TastySpleen
